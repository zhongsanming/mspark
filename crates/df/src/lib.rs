//! Compile-time dataflow.
//!
//! A **node is a data type**. Implementing [`Node`] for `T` declares
//!
//! * `type Deps` — the nodes `T` is computed from (a tuple of `Node` types), and
//! * `Node::compute` — how to build the whole table `Vec<T>` from those deps.
//!
//! [`resolve::<T>`] walks that statically-known graph, computes every node at
//! most once (memoised in a [`Store`]), and hands back a shared `Arc<Vec<T>>`.
//! There is no runtime registry and no runtime graph discovery: the dependency
//! edges, their ordering and their type matching are all fixed at monomorphisation
//! time. Only leaf inputs (files), the chip, and which targets you ask for are
//! runtime values.
//!
//! Because `resolve` is ordinary recursion over `T::Deps`, the topological order
//! falls out for free and DAG fan-out is computed once, not once per consumer.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow};

/// Immutable environment shared by every node.
///
/// Everything that must be parsed *before* the graph runs (device info, id→text
/// maps, paths) is loaded into the context once. Nodes never mutate it, and go
/// through typed accessors only.
pub struct Ctx {
    root: PathBuf,
    device: Option<PathBuf>,
    data: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Ctx {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            device: None,
            data: HashMap::new(),
        }
    }

    /// Attach the per-device directory this run is scoped to.
    pub fn with_device(mut self, device: impl Into<PathBuf>) -> Self {
        self.device = Some(device.into());
        self
    }

    /// The `PROF_*` directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn device_dir(&self) -> Result<&Path> {
        self.device
            .as_deref()
            .ok_or_else(|| anyhow!("context has no device directory"))
    }

    /// `<device>/data`, where the raw device streams live.
    pub fn device_data_dir(&self) -> Result<PathBuf> {
        Ok(self.device_dir()?.join("data"))
    }

    /// `<PROF>/host/data`, where the raw host streams live.
    pub fn host_data_dir(&self) -> PathBuf {
        self.root.join("host").join("data")
    }

    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) {
        self.data.insert(TypeId::of::<T>(), Box::new(value));
    }

    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.data
            .get(&TypeId::of::<T>())
            .map(|boxed| boxed.downcast_ref::<T>().expect("TypeId collision"))
    }

    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&T> {
        self.get::<T>().ok_or_else(|| {
            anyhow!(
                "context item `{}` was not loaded",
                std::any::type_name::<T>()
            )
        })
    }
}

/// A statically-typed list of node dependencies.
///
/// Implemented for `()` and for tuples of [`Node`] types. `Refs` is the tuple of
/// already-resolved `Arc<Vec<_>>` handles handed to [`Node::compute`].
pub trait DepList {
    type Refs;
    fn resolve_all(store: &mut Store, ctx: &Ctx) -> Result<Self::Refs>;
}

/// A node: it *is* the row type it produces.
pub trait Node: Sized + Send + Sync + 'static {
    /// Stable name used in error context and diagnostics.
    const NAME: &'static str;

    /// Dependencies, resolved at compile time.
    type Deps: DepList + DepNames;

    /// Dependency names derived from `Deps`; used by [`graph!`].
    const DEP_NAMES: &'static [&'static str] = <Self::Deps as DepNames>::NAMES;

    /// Build this node's full table from its already-computed dependencies.
    fn compute(ctx: &Ctx, deps: <Self::Deps as DepList>::Refs) -> Result<Vec<Self>>;

    /// Emit rows one at a time instead of returning the whole table.
    ///
    /// A node whose table is only ever written to a file ("sink" consumers) can
    /// override this to avoid materialising it: `task_time`/`op_summary` tables
    /// are hundreds of thousands of rows of strings, and holding them all cost
    /// more than every input stream combined. The default collects via
    /// [`Node::compute`], so most nodes need nothing.
    ///
    /// Nodes that override this must still implement `compute` (typically by
    /// collecting from this function) so that `resolve` keeps working for any
    /// other consumer.
    fn emit(
        ctx: &Ctx,
        deps: <Self::Deps as DepList>::Refs,
        out: &mut dyn FnMut(Self) -> Result<()>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        for row in Self::compute(ctx, deps)? {
            out(row)?;
        }
        Ok(())
    }
}

/// Memoisation store for computed tables. Keys are node types.
#[derive(Default)]
pub struct Store {
    data: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    fn get<T: Node>(&self) -> Option<Arc<Vec<T>>> {
        self.data.get(&TypeId::of::<T>()).map(|boxed| {
            boxed
                .clone()
                .downcast::<Vec<T>>()
                .expect("TypeId collision")
        })
    }

    fn put<T: Node>(&mut self, value: Arc<Vec<T>>) {
        self.data.insert(TypeId::of::<T>(), value);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn contains<T: Node>(&self) -> bool {
        self.data.contains_key(&TypeId::of::<T>())
    }
}

/// Resolve `T` and, transitively, everything it depends on.
///
/// Failure is allowed at every step; a failed node is not memoised, so the error
/// propagates to the caller with the node name attached.
/// Resolve a node's dependencies and hand its rows to `out` without memoising
/// the table.
///
/// Use this from reports whose only consumer is a writer: the node's `emit` is
/// called instead of `compute`, so a large table never exists as a `Vec`. The
/// dependencies are still memoised, so other artifacts keep their caching.
pub fn resolve_into<T: Node>(
    store: &mut Store,
    ctx: &Ctx,
    out: &mut dyn FnMut(T) -> Result<()>,
) -> Result<()> {
    let deps = <T::Deps as DepList>::resolve_all(store, ctx)
        .with_context(|| format!("resolving dependencies of `{}`", T::NAME))?;
    T::emit(ctx, deps, out).with_context(|| format!("computing `{}`", T::NAME))
}

pub fn resolve<T: Node>(store: &mut Store, ctx: &Ctx) -> Result<Arc<Vec<T>>> {
    if let Some(cached) = store.get::<T>() {
        return Ok(cached);
    }
    let deps = <T::Deps as DepList>::resolve_all(store, ctx)
        .with_context(|| format!("resolving dependencies of `{}`", T::NAME))?;
    let value =
        Arc::new(T::compute(ctx, deps).with_context(|| format!("computing `{}`", T::NAME))?);
    store.put::<T>(value.clone());
    Ok(value)
}

impl DepList for () {
    type Refs = ();

    fn resolve_all(_: &mut Store, _: &Ctx) -> Result<()> {
        Ok(())
    }
}

macro_rules! impl_deplist {
    ($($N:ident),+) => {
        impl<$($N: Node),+> DepList for ($($N,)+) {
            type Refs = ($(Arc<Vec<$N>>,)+);

            #[allow(non_snake_case)]
            fn resolve_all(store: &mut Store, ctx: &Ctx) -> Result<Self::Refs> {
                Ok(($(resolve::<$N>(store, ctx)?,)+))
            }
        }
    };
}

impl_deplist!(A);
impl_deplist!(A, B);
impl_deplist!(A, B, C);
impl_deplist!(A, B, C, D);
impl_deplist!(A, B, C, D, E);
impl_deplist!(A, B, C, D, E, F);
impl_deplist!(A, B, C, D, E, F, G);
impl_deplist!(A, B, C, D, E, F, G, H);
impl_deplist!(A, B, C, D, E, F, G, H, I);
impl_deplist!(A, B, C, D, E, F, G, H, I, J);
impl_deplist!(A, B, C, D, E, F, G, H, I, J, K);
impl_deplist!(A, B, C, D, E, F, G, H, I, J, K, L);

// ---------------------------------------------------------------------------
// Compile-time graph validation
// ---------------------------------------------------------------------------

/// Names of a dependency tuple.
///
/// This lets [`graph!`] recover each node's edges from its `Deps` type, so the
/// edges are declared exactly once (in the `impl Node`) and never duplicated in
/// a separate manifest.
pub trait DepNames {
    const NAMES: &'static [&'static str];
}

impl DepNames for () {
    const NAMES: &'static [&'static str] = &[];
}

macro_rules! impl_dep_names {
    ($($N:ident),+) => {
        impl<$($N: Node),+> DepNames for ($($N,)+) {
            const NAMES: &'static [&'static str] = &[$(<$N as Node>::NAME),+];
        }
    };
}

impl_dep_names!(A);
impl_dep_names!(A, B);
impl_dep_names!(A, B, C);
impl_dep_names!(A, B, C, D);
impl_dep_names!(A, B, C, D, E);
impl_dep_names!(A, B, C, D, E, F);
impl_dep_names!(A, B, C, D, E, F, G);
impl_dep_names!(A, B, C, D, E, F, G, H);
impl_dep_names!(A, B, C, D, E, F, G, H, I);
impl_dep_names!(A, B, C, D, E, F, G, H, I, J);
impl_dep_names!(A, B, C, D, E, F, G, H, I, J, K);
impl_dep_names!(A, B, C, D, E, F, G, H, I, J, K, L);

/// Upper bound on the number of nodes the const check can walk.
pub const MAX_NODES: usize = 128;

const fn str_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// Reject unknown edges and dependency cycles at compile time.
///
/// Evaluated in a `const` context by [`graph!`], so a cycle is a build error
/// rather than a monomorphisation overflow.
pub const fn check_acyclic(graph: &[(&str, &[&str])]) {
    let n = graph.len();
    assert!(n <= MAX_NODES, "node graph exceeds MAX_NODES");
    let mut a = 0;
    while a < n {
        let mut b = a + 1;
        while b < n {
            assert!(
                !str_eq(graph[a].0, graph[b].0),
                "duplicate node name in NODE_GRAPH"
            );
            b += 1;
        }
        a += 1;
    }
    let mut start = 0;
    while start < n {
        let mut seen = [false; MAX_NODES];
        let mut stack = [0usize; MAX_NODES];
        let mut top = 1usize;
        stack[0] = start;
        seen[start] = true;
        while top > 0 {
            top -= 1;
            let node = stack[top];
            let deps = graph[node].1;
            let mut d = 0;
            while d < deps.len() {
                let mut k = 0;
                let mut found = n;
                while k < n {
                    if str_eq(graph[k].0, deps[d]) {
                        found = k;
                    }
                    k += 1;
                }
                assert!(found < n, "graph edge points at an unknown node");
                assert!(found != start, "dependency cycle detected in node graph");
                if !seen[found] {
                    seen[found] = true;
                    stack[top] = found;
                    top += 1;
                }
                d += 1;
            }
        }
        start += 1;
    }
}

/// Declare the node graph; fails the build on an unknown edge or a cycle.
///
/// ```ignore
/// mspark_df::graph!(TaskTimeRow, OpSummaryRow, TimelineTask);
/// ```
#[macro_export]
macro_rules! graph {
    ($($node:ty),+ $(,)?) => {
        /// Static node graph: `(name, dependencies)` for every node.
        pub const NODE_GRAPH: &[(&str, &[&str])] = &[
            $( (<$node as $crate::Node>::NAME, <$node as $crate::Node>::DEP_NAMES), )+
        ];
        const _: () = $crate::check_acyclic(NODE_GRAPH);
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct Leaf(u32);

    impl Node for Leaf {
        const NAME: &'static str = "leaf";
        type Deps = ();

        fn compute(_: &Ctx, (): ()) -> Result<Vec<Self>> {
            Ok(vec![Leaf(7)])
        }
    }

    #[derive(Debug, PartialEq)]
    struct Mid(u32);

    impl Node for Mid {
        const NAME: &'static str = "mid";
        type Deps = (Leaf,);

        fn compute(_: &Ctx, (leaf,): (Arc<Vec<Leaf>>,)) -> Result<Vec<Self>> {
            Ok(vec![Mid(leaf[0].0 + 1)])
        }
    }

    #[derive(Debug, PartialEq)]
    struct Top(u32);

    impl Node for Top {
        const NAME: &'static str = "top";
        // Depends on `Leaf` twice (via `Mid` and directly): fan-out must compute once.
        type Deps = (Mid, Leaf);

        fn compute(_: &Ctx, (mid, leaf): (Arc<Vec<Mid>>, Arc<Vec<Leaf>>)) -> Result<Vec<Self>> {
            Ok(vec![Top(mid[0].0 + leaf[0].0)])
        }
    }

    #[test]
    fn resolves_in_dependency_order_and_memoises() {
        let ctx = Ctx::new(".");
        let mut store = Store::new();
        let top = resolve::<Top>(&mut store, &ctx).unwrap();
        assert_eq!(&*top, &[Top(15)]);
        // Leaf, Mid, Top: `Leaf` was requested twice but stored once.
        assert_eq!(store.len(), 3);
        assert!(store.contains::<Leaf>());
        assert!(store.contains::<Mid>());
        assert!(store.contains::<Top>());
    }

    #[test]
    fn ctx_is_typed() {
        let mut ctx = Ctx::new("/tmp");
        ctx.insert(42u32);
        assert_eq!(ctx.get::<u32>(), Some(&42));
        assert_eq!(ctx.get::<u64>(), None);
        assert!(ctx.require::<u64>().is_err());
    }
}
