use std::ops::Deref;

/// The arguments of a collection write: one element kept inline, more in a `Vec`.
///
/// Most `LPUSH`/`RPUSH`/`ZADD` calls carry a single element; keeping it inline saves the
/// command's only heap allocation. Reads go through the slice either way.
#[derive(Debug, Clone)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    /// Keeps the elements `keep` accepts, in order; `keep` sees every element once.
    pub fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        match self {
            Self::One(value) => {
                if !keep(value) {
                    *self = Self::Many(Vec::new());
                }
            }
            Self::Many(values) => values.retain(keep),
        }
    }

    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> OneOrMany<U> {
        match self {
            Self::One(value) => OneOrMany::One(f(value)),
            Self::Many(values) => OneOrMany::Many(values.into_iter().map(f).collect()),
        }
    }

    pub fn into_vec(self) -> Vec<T> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

impl<T> Deref for OneOrMany<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        match self {
            Self::One(value) => std::slice::from_ref(value),
            Self::Many(values) => values,
        }
    }
}

impl<T> From<Vec<T>> for OneOrMany<T> {
    /// A one-element `Vec` stays a `Vec`: converting it would not save its allocation.
    fn from(values: Vec<T>) -> Self {
        Self::Many(values)
    }
}

impl<T: PartialEq> PartialEq for OneOrMany<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<T> FromIterator<T> for OneOrMany<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut iter = iter.into_iter();

        match (iter.next(), iter.next()) {
            (None, _) => Self::Many(Vec::new()),
            (Some(first), None) => Self::One(first),
            (Some(first), Some(second)) => {
                let mut values = Vec::with_capacity(2 + iter.size_hint().0);

                values.push(first);
                values.push(second);
                values.extend(iter);
                Self::Many(values)
            }
        }
    }
}

impl<'v, T> IntoIterator for &'v OneOrMany<T> {
    type Item = &'v T;
    type IntoIter = std::slice::Iter<'v, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::OneOrMany;

    #[test]
    fn collects_one_element_inline_and_more_into_a_vec() {
        let one: OneOrMany<u8> = [7].into_iter().collect();
        let two: OneOrMany<u8> = [1, 2].into_iter().collect();
        let none: OneOrMany<u8> = std::iter::empty().collect();

        assert!(matches!(one, OneOrMany::One(7)));
        assert_eq!(&*two, &[1, 2]);
        assert!(none.is_empty());
        assert_eq!(one, OneOrMany::Many(vec![7]));
    }

    #[test]
    fn retain_keeps_order_and_empties_a_single_element() {
        let mut one = OneOrMany::One(3);
        let mut many = OneOrMany::Many(vec![1, 2, 3, 4]);
        let mut seen = Vec::new();

        one.retain(|value| *value != 3);
        many.retain(|value| {
            seen.push(*value);
            value % 2 == 0
        });

        assert!(one.is_empty());
        assert_eq!(&*many, &[2, 4]);
        assert_eq!(seen, [1, 2, 3, 4]);
    }
}
