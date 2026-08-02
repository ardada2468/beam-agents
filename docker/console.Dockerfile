# The Beam Agents Console image: the UI bundle plus the service that serves it.
#
# Not one of the Beam worker images. `docker/sdk-harness.Dockerfile` and
# `examples/fraud_triage_dataflow/Dockerfile` are SDK harnesses built on Beam's
# own bases, and everything they document about protobuf pinning exists because
# those bases ship a conflicting protobuf runtime. This image starts from stock
# `python:3.11-slim`, resolves `apache-beam[gcp]` and `protobuf` from this
# repo's own metadata, and therefore has no such conflict to pin around. Do not
# copy the protobuf pin here: there is nothing to override.
#
# Two stages, for two different reasons:
#
# 1. `frontend` — Node builds `frontend/` into the bundle. Node exists only to
#    produce ~250 KB of static assets; nothing at runtime needs it, so it must
#    not survive into the final image (design D9).
# 2. `runtime` — the console service. Python deps first from `pyproject.toml`
#    alone, then the package, then the bundle last. That ordering is the whole
#    cache story: a frontend-only edit invalidates only the final COPY, and a
#    `src/`-only edit invalidates only the `--no-deps` install. Neither
#    re-resolves `apache-beam[gcp]`, which is by far the slowest step.
#
# Build context is the repository root:
#
#   docker build -f docker/console.Dockerfile -t beam-agents-console:1.0.0 .
#
# or, with the compose stack that also brings up the demo pipeline:
#
#   make console-build && make console-up

# ---------------------------------------------------------------------------
# Stage 1: build the UI bundle.
# ---------------------------------------------------------------------------

# Digest-pinned, the same discipline `docker/compose.yaml` applies to every
# service it pulls. Load-bearing here specifically because the bundle is a build
# artifact that is never committed and never reviewed: a floating `node:22` tag
# would let the bytes served by a rebuilt image change with no change in this
# repository, and there would be no diff anywhere showing it. The pin makes the
# bundle a function of the tree.
FROM node:22-bookworm-slim@sha256:f32b81066cde10a75dbac96646099533316d94bac4150c55da1636e1f0ffdc46 AS frontend

WORKDIR /frontend

# Manifest and lockfile BEFORE the sources: this layer depends only on the
# dependency graph, so editing a `.tsx` file reuses the installed
# `node_modules` from cache instead of re-fetching 183 packages.
# `npm ci` (not `install`) so the lockfile is authoritative — an image that
# silently floated a transitive dependency would defeat the digest pin above.
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

COPY frontend/ ./
# `tsc -b && vite build`. `vite.config.ts` writes the bundle to
# `../src/beam_agents/console/static`, i.e. outside this WORKDIR — which is
# deliberate (it is what makes a locally built wheel ship the UI) and means the
# output path here is /src/..., not /frontend/dist.
RUN npm run build

# ---------------------------------------------------------------------------
# Stage 2: the runtime image.
# ---------------------------------------------------------------------------

# Digest-pinned for the same reason every image in `docker/compose.yaml` is:
# the stack has to behave identically on a contributor's laptop and on a CI
# runner. It matters more than usual for a healthcheck-bearing image — the
# HEALTHCHECK below runs `python -c`, so the interpreter that decides whether
# this container is healthy is the one this digest names.
# 3.11 rather than 3.12: `requires-python = ">=3.11,<3.13"` and `.python-version`
# pins 3.11, so this is the interpreter the test suite runs against.
FROM python:3.11-slim-bookworm@sha256:b18992999dbe963a45a8a4da40ac2b1975be1a776d939d098c647482bcad5cba AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Third-party dependencies FIRST, resolved from `pyproject.toml` and nothing
# else, so this layer is invalidated only by a dependency edit.
#
# The stub package is what makes that possible: hatchling needs
# `src/beam_agents/` to exist to build a wheel, but copying the real `src/` here
# would make every source edit re-resolve `apache-beam[gcp]`. So a one-file stub
# stands in, its dependency closure (`console` for FastAPI/uvicorn/SSE,
# `console-ingest` for the Kafka and BigQuery readers) is installed, and the
# stub itself is removed. The dependency set is therefore never hand-copied into
# this file and cannot drift from `pyproject.toml`.
COPY pyproject.toml README.md /src/
RUN mkdir -p /src/src/beam_agents \
 && touch /src/src/beam_agents/__init__.py \
 && pip install --no-cache-dir "/src[console,console-ingest]" \
 && pip uninstall --yes beam-agents \
 && rm -rf /src/src

# The package itself, with `--no-deps`: every dependency was installed in the
# cached layer above from the same metadata, and re-resolving them here would
# undo the cache split for no benefit.
COPY src /src/src
RUN pip install --no-cache-dir --no-deps /src \
 && rm -rf /src

# The examples, importable under their own names.
#
# This image already runs a Beam pipeline — `console-demo` is a DirectRunner job
# over the fake provider — so carrying the quickstart beside it is not a new
# role for the image, it is the same role with a second pipeline that can reach
# a real model. It is what lets somebody evaluate the library with
# `docker compose` alone: no checkout, no Python toolchain, no `uv`.
#
# On `PYTHONPATH` rather than installed: Beam pickles by module reference, so
# `examples.quickstart.pipeline` has to resolve under exactly that name in
# whatever process runs the graph — here, this container.
COPY examples /app/examples
ENV PYTHONPATH=/app

# The UI bundle, copied LAST so a frontend change rebuilds this layer and
# nothing else.
#
# It lands in /app/static rather than being written into the installed package's
# `console/static/` directory. Both satisfy design D9; this one does not hardcode
# a site-packages path that moves with the interpreter version, and it exercises
# the `$BEAM_AGENTS_CONSOLE_STATIC` seam `resolve_static_dir()` documents rather
# than a path only this Dockerfile knows about.
COPY --from=frontend /src/beam_agents/console/static/ /app/static/
ENV BEAM_AGENTS_CONSOLE_STATIC=/app/static

# Non-root, with a fixed UID/GID so the named database volume's ownership is
# stable across rebuilds.
#
# `mkdir` + `chown` of the database directory is load-bearing, not tidiness:
# Docker seeds a *fresh* named volume from the image's content at that path,
# ownership included. Without an owned directory here the volume is created
# root-owned, the console cannot create its SQLite file, and the container
# crash-loops on a first `docker compose up` — the exact command this image
# exists to make work.
RUN groupadd --gid 1001 console \
 && useradd --uid 1001 --gid 1001 --home-dir /app --no-create-home --shell /usr/sbin/nologin console \
 && mkdir -p /var/lib/beam-agents-console \
 && chown -R console:console /var/lib/beam-agents-console /app
USER console

# Defaults chosen so `docker run` with no configuration is already correct.
# `HOST=0.0.0.0` overrides the CLI's own `127.0.0.1` default and is load-bearing:
# the CLI binds loopback because the console has no authentication and must not
# listen on a public interface by accident, but loopback *inside a container's
# network namespace* is unreachable from anywhere, including a published port.
# Publishing the port is the deliberate act here; the trusted-network caveat in
# `docs/console.md` is the compensating control.
ENV BEAM_AGENTS_CONSOLE_DB=/var/lib/beam-agents-console/console.db \
    BEAM_AGENTS_CONSOLE_HOST=0.0.0.0 \
    BEAM_AGENTS_CONSOLE_PORT=8787

EXPOSE 8787

# The database lives here; `docker/compose.console.yaml` mounts a named volume
# over it so records survive `down`/`up`. Declared as a comment rather than a
# VOLUME instruction on purpose: a VOLUME would make every `docker run` create
# an anonymous volume, which accumulates and hides the fact that persistence is
# a deployment choice.

# `python -c` rather than `curl`: the slim base has no curl, and adding one to
# the runtime image for a healthcheck would put a network client in the image
# for no other reason. Reads the port from the environment so overriding
# `BEAM_AGENTS_CONSOLE_PORT` does not silently leave the healthcheck probing
# 8787 forever. `/healthz` reports healthy without any ingest having occurred,
# so this goes green on an empty store — which is correct: the console is up.
# This is the ONLY healthcheck definition for the stack: `compose.console.yaml`
# does not restate it, and its `condition: service_healthy` reads this one.
HEALTHCHECK --interval=5s --timeout=5s --start-period=30s --retries=12 \
  CMD ["python", "-c", "import os,urllib.request;urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('BEAM_AGENTS_CONSOLE_PORT','8787')+'/healthz',timeout=4).read()"]

# No arguments: every flag falls back to a `BEAM_AGENTS_CONSOLE_*` variable, so
# configuration is `environment:` in compose rather than an overridden command.
CMD ["beam-agents-console"]
