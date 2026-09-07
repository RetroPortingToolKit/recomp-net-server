//! The one gate every client-supplied name passes through.
//!
//! Lifted out of `ws_lobby` so the Discord login path shares it rather than
//! growing a second copy: a rule re-implemented at a second site cannot
//! inherit fixes to the first.
//!
//! Two rules, and they are different on purpose:
//!
//! * **Hygiene** ([`sanitize`]) — drop control characters, cap, trim. Applied
//!   to anything a client sends that will be shown to another player.
//! * **Refusal** ([`is_refused`]) — a name that trips the word list is refused
//!   rather than masked. A chat line is a moment and a mask reads as one; a
//!   name sits in the seat table and in front of every line that player sends.

/// Characters, and bytes, a name may occupy.
///
/// Both, because clients store names in a fixed 64-byte field
/// (`PSX_LOBBY_NAME_LEN`, `RecompLauncherCNetplayOnlinePlayer::display_name`).
/// Cutting here, on a character boundary, is what keeps a client from cutting
/// mid-sequence: a char cap alone is not a byte cap when one emoji is four.
///
/// Note for the Discord path: Discord allows 32 characters, which is up to 128
/// bytes of UTF-8, so a CJK or emoji-heavy Discord name will not fit whole. It
/// is cut here, deliberately and on a boundary, rather than mangled downstream.
pub const NAME_MAX_CHARS: usize = 32;
pub const NAME_MAX_BYTES: usize = 63; // the 64-byte client field, less its NUL

/// Mechanical hygiene for one name: drop control characters, cap, trim.
/// `None` when nothing survives.
pub fn sanitize(s: Option<String>) -> Option<String> {
    let s = s?;
    let mut out = String::new();
    for c in s.chars().filter(|c| !c.is_control()).take(NAME_MAX_CHARS) {
        if out.len() + c.len_utf8() > NAME_MAX_BYTES {
            break;
        }
        out.push(c);
    }
    let out = out.trim();
    if out.is_empty() {
        None
    } else {
        Some(out.to_string())
    }
}

/// True when the name trips the shared word list, and so may not be used.
pub fn is_refused(name: &str) -> bool {
    crate::chat_filter::apply(name) != name
}

/// Hygiene and refusal together: `Some(name)` when it may be used as-is.
///
/// This is the form the Discord login path wants. A refused Discord name must
/// not block the login -- the player did not choose it here and cannot fix it
/// from inside the game -- so the caller falls back to another candidate
/// instead. See `identity::default_handle_for`.
pub fn acceptable(s: Option<String>) -> Option<String> {
    let n = sanitize(s)?;
    if is_refused(&n) {
        return None;
    }
    Some(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hygiene_drops_control_characters_and_trims() {
        assert_eq!(
            sanitize(Some("Ma\nri\tsa ".to_string())).as_deref(),
            Some("Marisa")
        );
        assert_eq!(sanitize(Some("   ".to_string())), None);
        assert_eq!(sanitize(None), None);
    }

    #[test]
    fn a_name_is_capped_in_characters_and_in_bytes() {
        let long = "a".repeat(200);
        let got = sanitize(Some(long)).unwrap();
        assert_eq!(got.chars().count(), NAME_MAX_CHARS);

        /* 32 four-byte characters is 128 bytes -- over the 64-byte field every
         * client stores this in, so the BYTE cap has to bind first. This is
         * exactly the Discord case: 32 characters is within Discord's limit
         * and outside ours. */
        let wide = "\u{1F600}".repeat(32);
        let got = sanitize(Some(wide)).unwrap();
        assert!(got.len() <= NAME_MAX_BYTES, "{} bytes", got.len());
        assert!(got.chars().all(|c| c == '\u{1F600}'));
    }

    #[test]
    fn acceptable_refuses_rather_than_masking() {
        assert_eq!(acceptable(Some("Scunthorpe".into())).as_deref(), Some("Scunthorpe"));
        assert_eq!(acceptable(Some("fuck".into())), None);
        /* Not masked: nothing with stars in it ever comes back out. */
        assert!(acceptable(Some("fuck".into())).is_none());
    }
}
