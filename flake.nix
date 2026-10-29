{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      rust-overlay,
      naersk,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        # Keep the dev shell on the same pinned toolchain as CI.
        # `override` replaces the toolchain file's components, so merge the
        # editor extras back in rather than dropping rustfmt/clippy.
        rustToolchain = builtins.fromTOML (builtins.readFile ./rust-toolchain.toml);
        rust = (pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
          extensions = rustToolchain.toolchain.components ++ [
            "rust-src"
            "rust-analyzer"
          ];
        };
        naersk' = pkgs.callPackage naersk { };
      in
      {
        packages.default = naersk'.buildPackage ./.;
        devShell =
          with pkgs;
          mkShell {
            name = "mspark";
            buildInputs = [ rust ];
          };
      }
    );
}
