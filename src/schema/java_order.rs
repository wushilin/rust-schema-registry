//! Reproduce `java.util.HashMap`/`HashSet` iteration order.
//!
//! Confluent's JSON Schema and Protobuf diff code walks `HashSet`s, so the
//! order of compatibility messages is Java's hash-bucket order. Emulating it
//! lets our messages match Confluent's byte for byte.
//!
//! A key's final bucket is `spread(hash) & (capacity - 1)`; keys sharing a
//! bucket keep insertion order across resizes (Java splits buckets
//! order-preservingly), so the iteration order is a stable sort by bucket.
//! (Treeified buckets, >8 collisions, are not modeled.)

pub trait JavaHash {
    fn java_hash(&self) -> i32;
}

impl JavaHash for str {
    fn java_hash(&self) -> i32 {
        let mut h: i32 = 0;
        for u in self.encode_utf16() {
            h = h.wrapping_mul(31).wrapping_add(u as i32);
        }
        h
    }
}

impl JavaHash for String {
    fn java_hash(&self) -> i32 {
        self.as_str().java_hash()
    }
}

impl JavaHash for u32 {
    fn java_hash(&self) -> i32 {
        *self as i32
    }
}

fn spread(h: i32) -> u32 {
    let h = h as u32;
    h ^ (h >> 16)
}

fn table_size_for(cap: usize) -> usize {
    cap.max(1).next_power_of_two()
}

/// Simulates a Java HashSet/HashMap. `initial` is the table capacity.
pub struct JavaSet<K> {
    keys: Vec<K>,
    cap: usize,
}

impl<K: JavaHash + PartialEq + Clone> JavaSet<K> {
    /// `new HashMap<>()` / `new HashSet<>()`
    pub fn new() -> Self {
        Self { keys: Vec::new(), cap: 16 }
    }

    /// `new HashSet<>(collection)`
    pub fn from_collection(items: &[K]) -> Self {
        let cap = table_size_for(((items.len() as f32 / 0.75) as usize + 1).max(16));
        let mut s = Self { keys: Vec::new(), cap };
        s.extend(items);
        s
    }

    pub fn insert(&mut self, k: K) {
        if self.keys.contains(&k) {
            return;
        }
        self.keys.push(k);
        if self.keys.len() > self.cap * 3 / 4 {
            self.cap *= 2;
        }
    }

    pub fn extend(&mut self, items: &[K]) {
        for k in items {
            self.insert(k.clone());
        }
    }

    /// Iteration order.
    pub fn ordered(&self) -> Vec<K> {
        let mask = (self.cap - 1) as u32;
        let mut v: Vec<(u32, usize, K)> =
            self.keys.iter().enumerate().map(|(i, k)| (spread(k.java_hash()) & mask, i, k.clone())).collect();
        v.sort_by_key(|(b, i, _)| (*b, *i));
        v.into_iter().map(|(_, _, k)| k).collect()
    }
}

impl<K: JavaHash + PartialEq + Clone> Default for JavaSet<K> {
    fn default() -> Self {
        Self::new()
    }
}

/// Iteration order of a `ConcurrentHashMap` filled by `put` in the given
/// order. Same bucket function as `HashMap`, but the table doubles as soon
/// as the element count reaches 3/4 of it (one insert earlier).
pub fn chm_order<K: JavaHash + PartialEq + Clone>(keys: &[K]) -> Vec<K> {
    let mut cap = 16usize;
    let mut seen: Vec<K> = Vec::new();
    for k in keys {
        if !seen.contains(k) {
            seen.push(k.clone());
            if seen.len() >= cap * 3 / 4 {
                cap *= 2;
            }
        }
    }
    let mask = (cap - 1) as u32;
    let mut v: Vec<(u32, usize, K)> =
        seen.into_iter().enumerate().map(|(i, k)| (spread(k.java_hash()) & mask, i, k)).collect();
    v.sort_by_key(|(b, i, _)| (*b, *i));
    v.into_iter().map(|(_, _, k)| k).collect()
}

/// Iteration order of a `HashMap` filled by `put` in the given order.
pub fn map_order<K: JavaHash + PartialEq + Clone>(keys: &[K]) -> Vec<K> {
    let mut s = JavaSet::new();
    s.extend(keys);
    s.ordered()
}

/// Order of `s = new HashSet<>(a.keySet()); s.addAll(b.keySet())` where `a`
/// and `b` are HashMaps filled in the given orders.
pub fn union_order<K: JavaHash + PartialEq + Clone>(a: &[K], b: &[K]) -> Vec<K> {
    let mut s = JavaSet::from_collection(&map_order(a));
    s.extend(&map_order(b));
    s.ordered()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_hash_matches_java() {
        // Values from java.lang.String#hashCode.
        assert_eq!("".java_hash(), 0);
        assert_eq!("a".java_hash(), 97);
        assert_eq!("hello".java_hash(), 99162322);
        assert_eq!("polygenelubricants".java_hash(), i32::MIN);
    }

    #[test]
    fn small_integers_iterate_ascending() {
        assert_eq!(map_order(&[3u32, 1, 2]), vec![1, 2, 3]);
        // 1 and 17 collide in a 16-bucket table: insertion order is kept.
        assert_eq!(map_order(&[17u32, 1, 2]), vec![17, 1, 2]);
    }
}
