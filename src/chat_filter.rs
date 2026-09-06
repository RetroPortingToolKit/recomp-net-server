//! Chat profanity / slur filter -- the server half of one shared rule set.
//!
//! The list (`data/chat_filter_words.txt`, a verbatim copy of recomp-net's
//! `data/chat_filter_words.txt`) and the matching rules are the same ones
//! every client applies in C (`lib/recomp-net/src/chat/rnet_chat_filter.c`),
//! so a line reads the same whether it came through this server, an older
//! server, or a LAN room with no server at all. Change the rules in both
//! places or in neither; the unit tests below are the C test file's cases.
//!
//! Folding: lower case, Latin-1 / Latin Extended-A diacritics stripped,
//! Cyrillic and Greek lower-cased, full-width ASCII narrowed, leetspeak
//! undone. Matching tolerates repeated letters ("fuuuck") and, for words of
//! four or more letters, separators between letters ("f.u.c.k"). Whole-word
//! entries need a non-letter (judged on the ORIGINAL character) on both
//! sides; entries prefixed `~` match anywhere. A hit becomes one `*` per
//! character.
//!
//! `CHAT_FILTER=0` turns it off; `CHAT_FILTER_EXTRA_PATH` names a file of
//! extra entries in the same format, appended at startup.

use std::sync::OnceLock;

const BUILTIN_LIST: &str = include_str!("../data/chat_filter_words.txt");

struct Word {
    cp: Vec<char>,
    anywhere: bool,
}

pub struct ChatFilter {
    words: Vec<Word>,
    enabled: bool,
}

static FILTER: OnceLock<ChatFilter> = OnceLock::new();

/// Install the process-wide filter. Call once from main; `apply` falls back
/// to the built-in list when this was never called (tests, tools).
pub fn init(enabled: bool, extra_path: Option<&str>) {
    let mut text = String::from(BUILTIN_LIST);
    if let Some(p) = extra_path {
        match std::fs::read_to_string(p) {
            Ok(extra) => {
                text.push('\n');
                text.push_str(&extra);
            }
            Err(e) => tracing::warn!(path = p, error = %e, "chat filter: extra list not read"),
        }
    }
    let f = ChatFilter::parse(&text, enabled);
    tracing::info!(entries = f.words.len(), enabled, "chat filter");
    let _ = FILTER.set(f);
}

fn filter() -> &'static ChatFilter {
    FILTER.get_or_init(|| ChatFilter::parse(BUILTIN_LIST, true))
}

/// Mask profanity in `text`. Returns the text unchanged when nothing matched
/// or the filter is off.
pub fn apply(text: &str) -> String {
    filter().mask(text)
}

pub fn enabled() -> bool {
    filter().enabled
}

impl ChatFilter {
    fn parse(list: &str, enabled: bool) -> ChatFilter {
        let mut words = Vec::new();
        for raw in list.lines() {
            let line = raw.trim_end_matches('\r');
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (anywhere, body) = match line.strip_prefix('~') {
                Some(rest) => (true, rest),
                None => (false, line),
            };
            let cp: Vec<char> = body.chars().filter(|c| *c != ' ').map(fold).collect();
            if !cp.is_empty() {
                words.push(Word { cp, anywhere });
            }
        }
        ChatFilter { words, enabled }
    }

    fn mask(&self, text: &str) -> String {
        if !self.enabled || self.words.is_empty() {
            return text.to_string();
        }
        let mut orig: Vec<char> = text.chars().collect();
        let folded: Vec<char> = orig.iter().map(|c| fold(*c)).collect();
        let n = orig.len();
        let mut hits = 0;
        let mut i = 0;
        while i < n {
            if !is_letter(folded[i]) {
                i += 1;
                continue;
            }
            let mut best_end = 0;
            for w in &self.words {
                if w.cp[0] != folded[i] {
                    continue;
                }
                let end = match_at(&folded, &orig, i, w);
                if end > best_end {
                    best_end = end;
                }
            }
            if best_end > 0 {
                for c in orig.iter_mut().take(best_end).skip(i) {
                    *c = '*';
                }
                hits += 1;
                i = best_end;
            } else {
                i += 1;
            }
        }
        if hits == 0 {
            return text.to_string();
        }
        orig.into_iter().collect()
    }
}

/// End (exclusive) of a match of `w` at `at`, or 0. Mirrors the C `match_at`.
fn match_at(t: &[char], orig: &[char], at: usize, w: &Word) -> usize {
    let n = t.len();
    let allow_sep = w.cp.len() >= 4;
    if !w.anywhere && at > 0 && is_letter(orig[at - 1]) {
        return 0;
    }
    let mut p = at;
    for (j, &want) in w.cp.iter().enumerate() {
        if j > 0 && allow_sep {
            while p < n && is_separator(t[p]) {
                p += 1;
            }
        }
        if p >= n || t[p] != want {
            return 0;
        }
        p += 1;
        if j + 1 >= w.cp.len() || w.cp[j + 1] != want {
            while p < n && t[p] == want {
                p += 1;
            }
        }
    }
    if !w.anywhere && p < n && is_letter(orig[p]) {
        return 0;
    }
    p
}

fn fold_latin1(c: char) -> char {
    const MAP: &[u8] = b"aaaaaaaceeeeiiiidnooooo/ouuuuytsaaaaaaaceeeeiiiidnooooo/ouuuuyty";
    let idx = (c as u32 - 0xC0) as usize;
    let m = MAP[idx];
    if m == b'/' {
        return c;
    }
    if c == '\u{DE}' || c == '\u{FE}' {
        return 't';
    }
    m as char
}

fn fold(c: char) -> char {
    let u = c as u32;
    if u < 0x80 {
        return match c {
            'A'..='Z' => ((u + 32) as u8) as char,
            '0' => 'o',
            '1' => 'i',
            '3' => 'e',
            '4' => 'a',
            '5' => 's',
            '7' => 't',
            '@' => 'a',
            '$' => 's',
            '!' => 'i',
            '|' => 'l',
            '+' => 't',
            _ => c,
        };
    }
    if (0xC0..=0xFF).contains(&u) {
        return fold_latin1(c);
    }
    if (0x100..=0x17F).contains(&u) {
        const BASE: &[u8] = b"aaaaaaccccccccddddeeeeeeeeeegggggggghhhhiiiiiiiiiijjjjkkklllllllllllnnnnnnnnnoooooooorrrrrrssssssssttttttuuuuuuuuuuuuwwyyyzzzzzzs";
        let idx = (u - 0x100) as usize;
        return if idx < BASE.len() { BASE[idx] as char } else { c };
    }
    if (0x410..=0x42F).contains(&u) {
        return char::from_u32(u + 0x20).unwrap_or(c);
    }
    match u {
        0x401 | 0x451 => return '\u{435}',
        0x404 => return '\u{454}',
        0x406 => return '\u{456}',
        0x407 => return '\u{457}',
        0x490 => return '\u{491}',
        _ => {}
    }
    if (0x391..=0x3A9).contains(&u) {
        return char::from_u32(u + 0x20).unwrap_or(c);
    }
    match u {
        0x3C2 => return '\u{3C3}',
        0x3AC | 0x386 => return '\u{3B1}',
        0x3AD | 0x388 => return '\u{3B5}',
        0x3AE | 0x389 => return '\u{3B7}',
        0x3AF | 0x38A => return '\u{3B9}',
        0x3CC | 0x38C => return '\u{3BF}',
        0x3CD | 0x38E => return '\u{3C5}',
        0x3CE | 0x38F => return '\u{3C9}',
        _ => {}
    }
    if (0xFF01..=0xFF5E).contains(&u) {
        return fold(char::from_u32(u - 0xFF01 + 0x21).unwrap_or(c));
    }
    if u == 0x20AC {
        return 'e';
    }
    c
}

fn is_letter(c: char) -> bool {
    let u = c as u32;
    if u < 0x80 {
        return c.is_ascii_lowercase();
    }
    if (0x2000..=0x206F).contains(&u) || (0x3000..=0x303F).contains(&u) || (0xFF00..=0xFF0F).contains(&u) {
        return false;
    }
    !matches!(u, 0xA0 | 0xB7 | 0xBF | 0xA1)
}

fn is_separator(c: char) -> bool {
    matches!(c, ' ' | '.' | '-' | '_' | '*' | '\'' | ',' | '\u{2019}' | '\u{A0}')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f() -> ChatFilter {
        ChatFilter::parse(BUILTIN_LIST, true)
    }

    fn expect(input: &str, want: &str) {
        assert_eq!(f().mask(input), want, "input: {input}");
    }

    #[test]
    fn plain_case_repeats_leet_spacing() {
        expect("fuck", "****");
        expect("FUCK you", "**** you");
        expect("fuuuuck", "*******");
        expect("sh1t happens", "**** happens");
        expect("$hit", "****");
        expect("f.u.c.k", "*******");
        expect("f u c k off", "******* off");
        expect("ｆｕｃｋ", "****");
        expect("what the fuck?!", "what the ****?!");
        expect("asshole!", "*******!");
    }

    #[test]
    fn whole_word_boundaries() {
        expect("class assassin", "class assassin");
        expect("Scunthorpe", "Scunthorpe");
        expect("shitake mushrooms", "shitake mushrooms");
        expect("cumulative", "cumulative");
        expect("bass fishing", "bass fishing");
        expect("compute puta", "compute ****");
        expect("hello", "hello");
        expect("a s s", "a s s");
    }

    #[test]
    fn substring_and_scripts() {
        expect("n1gger", "******");
        expect("Sniggers", "S******s");
        expect("блять", "*****");
        expect("СУКА блин", "**** блин");
        expect("blyat", "*****");
        expect("死ね", "**");
        expect("お前死ね!", "お前**!");
        expect("傻逼", "**");
        expect("씨발 진짜", "** 진짜");
        expect("scheiße", "*******");
        expect("putain de merde", "****** de *****");
        expect("filho da puta", "*************");
        expect("ヽ(´ー｀)ノ", "ヽ(´ー｀)ノ");
        expect("gg 😀 fuck 😀", "gg 😀 **** 😀");
    }

    #[test]
    fn disabled_passes_through() {
        let off = ChatFilter::parse(BUILTIN_LIST, false);
        assert_eq!(off.mask("fuck"), "fuck");
    }
}
