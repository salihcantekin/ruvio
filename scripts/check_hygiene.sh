#!/usr/bin/env bash
# Scan a Ruvio checkout for text that must not reach the public repository:
# internal planning identifiers, Turkish text and test names that break each
# language's naming convention.
#
#   scripts/check_hygiene.sh [repo-root] [git-base-ref]
#
# With a base ref (for example origin/dev) only lines added since that ref are
# scanned, which is fast enough for every pull request; without one the whole
# tracked tree is scanned. `git grep` and `git diff` see tracked files only, so
# run `git add -N .` first to include new files. Exit 1 when anything is found.
set -u
export LC_ALL=C.UTF-8
root="${1:-.}"
base="${2:-}"
cd "$root" || exit 2

# .gitignore names the untracked planning folders, and this script holds the
# patterns themselves. Test data, lock files, images and JSON are not prose.
excludes=(':!.gitignore' ':!scripts/check_hygiene.sh' ':!*/tests/*' ':!*Tests/*' ':!*/test/*' ':!tests' ':!*_test.go' ':!*.test.ts' ':!test_*.py' ':!Cargo.lock' ':!*.lock' ':!*.png' ':!*.svg' ':!*.ico' ':!licenses' ':!site/data' ':!*.json' ':!*.patch')
# Planning ids and decision-history references.
ids='\b(T[0-9]{2}[-: ]|Faz ?[0-9]+|ADR[ -]?[0-9]+|STATUS\.md)|docs/(plan|adr)|plan/adr/'
# Letters that occur only in Turkish, and two very common Turkish words.
tr_chars='[ğışİŞĞ]|[Çç]ö|değer|için'
found=0

scan() { # label pattern
  local out
  if [ -n "$base" ]; then
    # Commits since the base, then changes not committed yet (including files
    # added with `git add -N`).
    out=$({ git diff -U0 "$base"...HEAD -- . "${excludes[@]}"; git diff -U0 HEAD -- . "${excludes[@]}"; } | grep -E '^\+[^+]' | grep -E "$2" | head -40)
  else
    out=$(git grep -nE "$2" -- . "${excludes[@]}" | head -40)
  fi
  if [ -n "$out" ]; then echo "== $1"; echo "$out"; found=1; fi
}

scan "planning/decision ids" "$ids"
scan "Turkish characters" "$tr_chars"
if [ -z "$base" ]; then
  if git ls-files | grep -qE '^docs/(plan|adr)/'; then echo "== docs/plan or docs/adr is tracked"; found=1; fi
fi

# Test names: Python snake_case, Go TestXxx, C# PascalCase. Rust names are
# checked by the compiler's non_snake_case lint (clippy runs in check.sh).
bad_py=$(git grep -nE '^\s*(async )?def test[A-Z]|def test_[a-z0-9_]*[A-Z]' -- 'clients/python' | head -20)
bad_go=$(git grep -nE '^func test[A-Za-z]|^func Test_' -- 'clients/go/*_test.go' | head -20)
bad_cs=$(git grep -nE 'public (async )?(Task|void) [a-z_]+[A-Za-z0-9_]*\(' -- 'clients/csharp/*Tests/*.cs' 'integrations/dotnet' 2>/dev/null | head -20)
for pair in "Python test names:$bad_py" "Go test names:$bad_go" "C# test names:$bad_cs"; do
  label="${pair%%:*}"; body="${pair#*:}"
  if [ -n "$body" ]; then echo "== $label"; echo "$body"; found=1; fi
done

[ "$found" = 0 ] && echo "hygiene: clean"
exit "$found"
