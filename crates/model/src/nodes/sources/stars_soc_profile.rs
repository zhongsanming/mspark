use anyhow::Result;
use mspark_df::{Ctx, Node};

/// One decoded `stars_soc_profile` record (SIO, Acc PMU, Chip Trans, Inter-SOC).
#[derive(Debug, Clone, Copy)]
pub struct SocProfileRecord(pub super::stars_soc_profile::SocProfileRaw);

impl Node for SocProfileRecord {
    const NAME: &'static str = "stars_soc_profile";
    type Deps = ();

    fn compute(ctx: &Ctx, (): ()) -> Result<Vec<Self>> {
        let bytes = crate::stream::read_stream(
            ctx,
            "stars_soc_profile.",
            super::stars_soc_profile::STARS_SOC_PROFILE_TRUNK,
        )?;
        Ok(super::stars_soc_profile::decode_stars_soc_profile(&bytes)?
            .into_iter()
            .map(SocProfileRecord)
            .collect())
    }
}

pub const STARS_SOC_PROFILE_TRUNK: usize = 64;

pub const FUNC_SOC_PROFILE_SIO: u16 = 0b011001;

pub const FUNC_SOC_PROFILE_ACC_PMU: u16 = 0b011010;

pub const FUNC_SOC_PROFILE_CHIP_TRANS: u16 = 0b011011;

pub const FUNC_SOC_PROFILE_INTER_SOC: u16 = 0b011100;

#[derive(Debug, Clone, Copy)]
pub enum SocProfileRaw {
    Sio {
        sys_cnt: u64,
        acc_id: u16,
        bandwidth: [u32; 8],
    },
    AccPmu {
        timestamp: u64,
        acc_id: u16,
        bandwidth: [u64; 2],
        ost: [u64; 2],
    },
    ChipTrans {
        sys_cnt: u64,
        acc_type: u16,
        event_id: u16,
        value1: u64,
        value2: u64,
    },
    InterSoc {
        sys_cnt: u64,
        l2_buffer_bw_level: u32,
        mata_bw_level: u32,
    },
}

pub fn decode_stars_soc_profile(bytes: &[u8]) -> Result<Vec<SocProfileRaw>> {
    let mut out = Vec::new();
    for chunk in bytes.as_chunks::<STARS_SOC_PROFILE_TRUNK>().0 {
        let u16_at = |o: usize| u16::from_le_bytes([chunk[o], chunk[o + 1]]);
        let u32_at =
            |o: usize| u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]]);
        let u64_at = |o: usize| {
            u64::from_le_bytes([
                chunk[o],
                chunk[o + 1],
                chunk[o + 2],
                chunk[o + 3],
                chunk[o + 4],
                chunk[o + 5],
                chunk[o + 6],
                chunk[o + 7],
            ])
        };
        let w0 = u16_at(0);
        match w0 & 0x3f {
            FUNC_SOC_PROFILE_SIO => out.push(SocProfileRaw::Sio {
                sys_cnt: u64_at(8),
                acc_id: u16_at(18),
                bandwidth: core::array::from_fn(|i| u32_at(32 + i * 4)),
            }),
            FUNC_SOC_PROFILE_ACC_PMU => out.push(SocProfileRaw::AccPmu {
                timestamp: u64_at(8),
                acc_id: u16_at(18),
                bandwidth: [u64_at(32), u64_at(40)],
                ost: [u64_at(48), u64_at(56)],
            }),
            FUNC_SOC_PROFILE_CHIP_TRANS => out.push(SocProfileRaw::ChipTrans {
                sys_cnt: u64_at(8),
                acc_type: (w0 >> 10) & 0x3f,
                event_id: u16_at(18) & 0x3f,
                value1: u64_at(32),
                value2: u64_at(40),
            }),
            FUNC_SOC_PROFILE_INTER_SOC => {
                let level = u16_at(32);
                out.push(SocProfileRaw::InterSoc {
                    sys_cnt: u64_at(8),
                    l2_buffer_bw_level: (level & 0x7) as u32,
                    mata_bw_level: ((level & 0x7f8) >> 3) as u32,
                });
            }
            _ => {}
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// NPU memory and module memory
// ---------------------------------------------------------------------------
