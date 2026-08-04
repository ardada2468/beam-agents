## ADDED Requirements

### Requirement: Console UI changes are gated by the package's full verification surface

Every pull request and every push to `main` that touches `frontend/` (or the frontend workflow file itself) SHALL run a CI job that installs dependencies faithfully from the committed lockfile (`npm ci`, never `npm install`) and then runs the package's lint, typecheck, and production build scripts as separate, distinguishable steps, so a red run names which surface broke. The job SHALL pin its Node version explicitly (or via a version file once one exists in `frontend/`) rather than floating on the runner default, and SHALL cache the npm store keyed on `frontend/package-lock.json`.

#### Scenario: A frontend change runs the lane

- **WHEN** a pull request modifies any file under `frontend/`
- **THEN** the frontend job runs `npm ci`, `npm run lint`, `npm run typecheck`, and `npm run build` against that change, and the PR cannot claim a green frontend check without all four succeeding

#### Scenario: Failures are attributable to a step

- **WHEN** the console UI has an ESLint violation, a type error, or a build break
- **THEN** the failing step (`Lint`, `Type check`, or `Build`) is individually red, rather than one aggregate step burying which surface failed

#### Scenario: Changes elsewhere do not pay for it

- **WHEN** a pull request touches only files outside `frontend/` and outside the frontend workflow file
- **THEN** the frontend job does not run, and no other lane's latency or flake surface grows because the console UI gained CI

#### Scenario: The lockfile is the only dependency authority

- **WHEN** `frontend/package.json` and `frontend/package-lock.json` disagree
- **THEN** `npm ci` fails the job rather than silently resolving new versions, so the tree that CI verified is the tree the lockfile records
