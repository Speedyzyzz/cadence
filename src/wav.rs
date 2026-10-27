//! src/wav.rs
//! Zero-dependency WAV file writer for PCM s16le mono audio.
//! Used by the chaos test suite to generate audible comparison artifacts.

use std::io::Write;
use std::path::Path;

/// Writes a standard RIFF/WAVE PCM s16le mono file.
pub fn write_wav_file(path: &Path, samples: &[i16], sample_rate: u32) -> std::io::Result<()> {
    let mut f = std::fs::File::create(path)?;
    let num_channels: u16 = 1;
    let bits_per_sample: u16 = 16;
    let byte_rate = sample_rate * (num_channels as u32) * (bits_per_sample as u32 / 8);
    let block_align = num_channels * (bits_per_sample / 8);
    let data_size = (samples.len() * 2) as u32;
    let file_size = 36 + data_size;

    // RIFF header
    f.write_all(b"RIFF")?;
    f.write_all(&file_size.to_le_bytes())?;
    f.write_all(b"WAVE")?;

    // fmt subchunk
    f.write_all(b"fmt ")?;
    f.write_all(&16u32.to_le_bytes())?; // Subchunk1Size (PCM)
    f.write_all(&1u16.to_le_bytes())?; // AudioFormat (1 = PCM)
    f.write_all(&num_channels.to_le_bytes())?;
    f.write_all(&sample_rate.to_le_bytes())?;
    f.write_all(&byte_rate.to_le_bytes())?;
    f.write_all(&block_align.to_le_bytes())?;
    f.write_all(&bits_per_sample.to_le_bytes())?;

    // data subchunk
    f.write_all(b"data")?;
    f.write_all(&data_size.to_le_bytes())?;
    for &s in samples {
        f.write_all(&s.to_le_bytes())?;
    }

    Ok(())
}
