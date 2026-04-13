# Backend Refactor Constitution

- Keep behavior unchanged unless a failing test proves the old behavior is wrong.
- Touch the smallest backend surface that resolves the identified issue.
- Run targeted tests before widening the change.
- Prefer moving logic into existing modules over adding new abstraction layers.
- Stop if the task expands beyond the stated backend scope.
