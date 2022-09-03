"""

Analysis:

1. NestProcess (has/hasnot operations) -> NestedBase (has operations) -> Nested -> Monad
2. GroupProcess (might has operations) -> GroupBase (has no operations) -> Group -> Monad

Summary:

- NOTE: Operations method at NestProcess & GroupProcess should not affect
the selective inheritence at the level of NestedBase & GroupBase

"""
