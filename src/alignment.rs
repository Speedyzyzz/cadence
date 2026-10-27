//! src/alignment.rs
//! Prosodic & word-boundary alignment for cross-provider splice point selection.
//! Parses Cartesia Sonic word_timestamps and ElevenLabs character alignment arrays,
//! builds a per-provider temporal index, and finds the optimal splice point at
//! a word boundary's low-energy RMS valley to prevent repeated syllables.

use crate::protocol::SampleRate;
use serde_json::Value;

/// A single word's temporal position within the PCM stream.
#[derive(Debug, Clone)]
pub struct WordBoundary {
    pub word: String,
    pub start_sample: usize,
    pub end_sample: usize,
    pub start_seconds: f64,
    pub end_seconds: f64,
}

/// Result of splice point computation.
#[derive(Debug, Clone)]
pub struct SplicePoint {
    /// The exact sample offset in the standby ring buffer to begin splicing from.
    pub standby_sample_offset: usize,
    /// The word index in the standby alignment where the splice begins.
    pub word_index: usize,
    /// Fractional progress within the word at the splice point (ρ ∈ [0, 1]).
    pub intra_word_progress: f64,
}

/// Per-provider word alignment index updated as timestamp metadata arrives from TTS JSON frames.
#[derive(Debug)]
pub struct WordAlignmentIndex {
    sample_rate: SampleRate,
    words: Vec<WordBoundary>,
    /// Cumulative total samples received across all chunks for this utterance.
    total_samples_received: usize,
    /// Sample offset of the first audio chunk in the current alignment context.
    /// Used to convert provider-relative timestamps to ring-buffer-relative offsets.
    chunk_base_offset: usize,
}

impl WordAlignmentIndex {
    pub fn new(sample_rate: SampleRate) -> Self {
        Self {
            sample_rate,
            words: Vec::with_capacity(64),
            total_samples_received: 0,
            chunk_base_offset: 0,
        }
    }

    pub fn word_count(&self) -> usize {
        self.words.len()
    }

    pub fn words(&self) -> &[WordBoundary] {
        &self.words
    }

    pub fn total_samples_received(&self) -> usize {
        self.total_samples_received
    }

    /// Updates the total sample count when a new PCM chunk is ingested.
    pub fn advance_sample_count(&mut self, sample_count: usize) {
        self.total_samples_received += sample_count;
    }

    /// Resets the index for a new utterance.
    pub fn reset(&mut self) {
        self.words.clear();
        self.total_samples_received = 0;
        self.chunk_base_offset = 0;
    }

    /// Converts a time in seconds to an absolute sample offset from utterance start.
    #[inline]
    fn seconds_to_samples(&self, seconds: f64) -> usize {
        (seconds * self.sample_rate.as_u32() as f64).round() as usize
    }

    /// Ingests Cartesia Sonic word-level timestamp metadata.
    /// Expected JSON format:
    /// ```json
    /// {
    ///   "type": "timestamps",
    ///   "word_timestamps": {
    ///     "words": ["Hello", "world"],
    ///     "start": [0.0, 0.35],
    ///     "end": [0.31, 0.62]
    ///   }
    /// }
    /// ```
    pub fn ingest_cartesia_timestamps(&mut self, json: &Value) {
        let wt = match json.get("word_timestamps") {
            Some(v) => v,
            None => return,
        };

        let words_arr = match wt.get("words").and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return,
        };
        let starts_arr = match wt.get("start").and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return,
        };
        let ends_arr = match wt.get("end").and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return,
        };

        let count = words_arr.len().min(starts_arr.len()).min(ends_arr.len());

        for i in 0..count {
            let word = match words_arr[i].as_str() {
                Some(w) => w.to_string(),
                None => continue,
            };
            let start_sec = starts_arr[i].as_f64().unwrap_or(0.0);
            let end_sec = ends_arr[i].as_f64().unwrap_or(start_sec);

            let start_sample = self.seconds_to_samples(start_sec) + self.chunk_base_offset;
            let end_sample = self.seconds_to_samples(end_sec) + self.chunk_base_offset;

            // Avoid duplicates if the same timestamp set is received multiple times
            if self.words.last().map_or(true, |last| last.start_sample != start_sample) {
                self.words.push(WordBoundary {
                    word,
                    start_sample,
                    end_sample,
                    start_seconds: start_sec,
                    end_seconds: end_sec,
                });
            }
        }
    }

    /// Ingests ElevenLabs character-level alignment metadata and aggregates into word boundaries.
    /// Expected JSON format:
    /// ```json
    /// {
    ///   "alignment": {
    ///     "characters": ["H", "e", "l", "l", "o", " ", "w", "o", "r", "l", "d"],
    ///     "character_start_times_seconds": [0.0, 0.08, 0.12, 0.16, 0.22, 0.30, 0.35, 0.40, 0.44, 0.48, 0.52],
    ///     "character_end_times_seconds": [0.08, 0.12, 0.16, 0.22, 0.30, 0.35, 0.40, 0.44, 0.48, 0.52, 0.58]
    ///   }
    /// }
    /// ```
    pub fn ingest_elevenlabs_alignment(&mut self, json: &Value) {
        let alignment = match json
            .get("normalized_alignment")
            .or_else(|| json.get("alignment"))
        {
            Some(v) => v,
            None => return,
        };

        let chars_arr = match alignment.get("characters").and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return,
        };
        let starts_arr = match alignment
            .get("character_start_times_seconds")
            .and_then(|v| v.as_array())
        {
            Some(a) => a,
            None => return,
        };
        let ends_arr = match alignment
            .get("character_end_times_seconds")
            .and_then(|v| v.as_array())
        {
            Some(a) => a,
            None => return,
        };

        let count = chars_arr.len().min(starts_arr.len()).min(ends_arr.len());

        let mut current_word = String::new();
        let mut word_start_sec: Option<f64> = None;
        let mut word_end_sec: f64 = 0.0;

        for i in 0..count {
            let ch = chars_arr[i].as_str().unwrap_or("");
            let start_sec = starts_arr[i].as_f64().unwrap_or(0.0);
            let end_sec = ends_arr[i].as_f64().unwrap_or(start_sec);

            if ch == " " || ch.is_empty() {
                // Whitespace: flush the current word if non-empty
                if !current_word.is_empty() {
                    let ws = word_start_sec.unwrap_or(0.0);
                    let start_sample = self.seconds_to_samples(ws) + self.chunk_base_offset;
                    let end_sample = self.seconds_to_samples(word_end_sec) + self.chunk_base_offset;

                    self.words.push(WordBoundary {
                        word: std::mem::take(&mut current_word),
                        start_sample,
                        end_sample,
                        start_seconds: ws,
                        end_seconds: word_end_sec,
                    });
                    word_start_sec = None;
                }
            } else {
                if word_start_sec.is_none() {
                    word_start_sec = Some(start_sec);
                }
                current_word.push_str(ch);
                word_end_sec = end_sec;
            }
        }

        // Flush the final word
        if !current_word.is_empty() {
            let ws = word_start_sec.unwrap_or(0.0);
            let start_sample = self.seconds_to_samples(ws) + self.chunk_base_offset;
            let end_sample = self.seconds_to_samples(word_end_sec) + self.chunk_base_offset;

            self.words.push(WordBoundary {
                word: current_word,
                start_sample,
                end_sample,
                start_seconds: ws,
                end_seconds: word_end_sec,
            });
        }
    }

    /// Given the current primary playback sample offset, finds the corresponding word in
    /// the standby alignment index and returns the optimal standby sample offset for splicing.
    ///
    /// If `standby_samples` is provided, the splice point is further refined by searching
    /// for a low-energy RMS valley within a ±2ms window around the computed offset.
    pub fn find_splice_point(
        &self,
        primary_sample_offset: usize,
        primary_alignment: &WordAlignmentIndex,
        standby_samples: Option<&[i16]>,
    ) -> Option<SplicePoint> {
        if primary_alignment.words.is_empty() || self.words.is_empty() {
            return None;
        }

        // Find which word the primary is currently inside
        let primary_word_idx = primary_alignment
            .words
            .iter()
            .position(|w| primary_sample_offset >= w.start_sample && primary_sample_offset <= w.end_sample)
            .or_else(|| {
                // If between words, find the nearest preceding word boundary
                primary_alignment
                    .words
                    .iter()
                    .rposition(|w| w.end_sample <= primary_sample_offset)
            })?;

        let primary_word = &primary_alignment.words[primary_word_idx];

        // Calculate intra-word progress ρ
        let word_duration_samples = primary_word.end_sample.saturating_sub(primary_word.start_sample).max(1);
        let progress_within_word = primary_sample_offset.saturating_sub(primary_word.start_sample);
        let rho = (progress_within_word as f64) / (word_duration_samples as f64);
        let rho = rho.clamp(0.0, 1.0);

        // Find the matching word in standby by index (or the closest available)
        let standby_word_idx = primary_word_idx.min(self.words.len().saturating_sub(1));
        let standby_word = &self.words[standby_word_idx];

        // Compute standby offset at the same fractional progress within the word
        let standby_word_duration = standby_word.end_sample.saturating_sub(standby_word.start_sample);
        let standby_raw_offset = standby_word.start_sample + ((standby_word_duration as f64 * rho) as usize);

        // Refine by searching for the nearest low-energy RMS valley
        let final_offset = if let Some(samples) = standby_samples {
            find_rms_valley(samples, standby_raw_offset, 2, self.sample_rate)
        } else {
            standby_raw_offset
        };

        Some(SplicePoint {
            standby_sample_offset: final_offset,
            word_index: standby_word_idx,
            intra_word_progress: rho,
        })
    }
}

/// Scans a ±window_ms region around `center` to find the sample offset with the
/// lowest short-term RMS energy (4-sample blocks), preferring inter-syllable dips
/// as natural splice points.
fn find_rms_valley(samples: &[i16], center: usize, window_ms: u32, rate: SampleRate) -> usize {
    let half_window = rate.samples_for_ms(window_ms) / 2;
    let block_size = 4usize;
    let start = center.saturating_sub(half_window);
    let end = (center + half_window).min(samples.len().saturating_sub(block_size));

    if start >= end || samples.len() < block_size {
        return center.min(samples.len().saturating_sub(1));
    }

    let mut min_rms = f64::MAX;
    let mut best_offset = center.min(end);

    let mut i = start;
    while i < end {
        let rms: f64 = samples[i..i + block_size]
            .iter()
            .map(|&s| (s as f64).powi(2))
            .sum::<f64>()
            .sqrt();
        if rms < min_rms {
            min_rms = rms;
            best_offset = i;
        }
        i += 1;
    }

    best_offset
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cartesia_word_timestamp_ingestion() {
        let mut idx = WordAlignmentIndex::new(SampleRate::Hz24000);
        let ts_json = json!({
            "type": "timestamps",
            "word_timestamps": {
                "words": ["Hello", "world"],
                "start": [0.0, 0.35],
                "end": [0.31, 0.62]
            }
        });
        idx.ingest_cartesia_timestamps(&ts_json);
        assert_eq!(idx.word_count(), 2);
        assert_eq!(idx.words()[0].word, "Hello");
        assert_eq!(idx.words()[0].start_sample, 0);
        assert_eq!(idx.words()[0].end_sample, 7440); // 0.31 * 24000 = 7440
        assert_eq!(idx.words()[1].word, "world");
        assert_eq!(idx.words()[1].start_sample, 8400); // 0.35 * 24000
    }

    #[test]
    fn test_elevenlabs_character_alignment_aggregation() {
        let mut idx = WordAlignmentIndex::new(SampleRate::Hz24000);
        let al_json = json!({
            "alignment": {
                "characters": ["H", "i", " ", "t", "h", "e", "r", "e"],
                "character_start_times_seconds": [0.0, 0.05, 0.10, 0.12, 0.16, 0.20, 0.24, 0.28],
                "character_end_times_seconds": [0.05, 0.10, 0.12, 0.16, 0.20, 0.24, 0.28, 0.32]
            }
        });
        idx.ingest_elevenlabs_alignment(&al_json);
        assert_eq!(idx.word_count(), 2);
        assert_eq!(idx.words()[0].word, "Hi");
        assert_eq!(idx.words()[1].word, "there");
    }

    #[test]
    fn test_rms_valley_finder() {
        // Create a simple signal with a quiet region in the middle
        let mut samples = vec![0i16; 480];
        for i in 0..480 {
            if i < 200 || i > 280 {
                samples[i] = 5000; // Loud
            } else {
                samples[i] = 50; // Quiet valley
            }
        }
        let valley = find_rms_valley(&samples, 240, 2, SampleRate::Hz24000);
        assert!(valley >= 200 && valley <= 280, "Valley should be in quiet region, got {valley}");
    }
}
