# 📓 Execution Roadmap & Daily Mentor Workbook
### Companion to: E-Commerce Behavioral Analytics Lakehouse — Implementation Guide v2.0
### Principal Data Engineer Mentorship Track

---

## HOW TO USE THIS DOCUMENT

This workbook is the document you actually follow, day by day. The Implementation Guide (v2.0) is your architecture reference — open it when this workbook tells you to read a specific section, but don't try to work from it directly day-to-day. This workbook tells you exactly what to do, in what order, and how to know you did it right.

Work through it top to bottom. Don't skip ahead, even if a task looks easy — the sequencing is deliberate, and later days assume earlier ones are genuinely done, not just "mostly working."

This is **Week 1 only**. It ends with a checkpoint. Confirm you've cleared it before I generate Week 2.

---

# WEEK 1 — Foundation: Environment & Ingestion Engineering

**Week 1 maps to:** Implementation Guide Phase 0 (Environment Setup) + Phase 1 (Automated Ingestion)
**By the end of this week you will have:** a working Docker stack with Airflow, Spark, MinIO, and Metabase talking to each other, and a fully automated, idempotent, retry-safe ingestion service pulling real clickstream data into your Raw zone.

---

## DAY 1 — Docker Foundations & Repository Setup

### 1. Daily Goal
Stand up a clean, version-controlled repository with the correct folder skeleton, and understand Docker well enough to know *why* the stack you're about to build is structured the way it is — not just how to run `docker-compose up`.

### 2. Learning First

**Concepts to understand before touching a terminal:**
- **Container vs. image** — an image is a frozen blueprint; a container is a running instance of it. You'll create several containers (Airflow, Spark, MinIO, Metabase) from images you don't build yourself, at least not yet.
- **Docker Compose vs. plain Docker** — Compose lets you define a *set* of containers and their relationships (networks, volumes, startup order) in one YAML file instead of long `docker run` commands. This is why real teams use it for local multi-service stacks.
- **Volumes** — the mechanism that lets a container's data survive a restart, and the mechanism that lets your local code folder appear *inside* a container (this is how Airflow will see your DAG files).
- **Networks** — by default, Compose puts all your services on one internal network so they can reach each other by service name (e.g., Spark can reach MinIO at `http://minio:9000`, not `localhost`). This trips up almost everyone the first time.

**Why this matters:** every mistake in Phase 0 costs you multiplied time later, because every other phase assumes the stack "just works." Get this right once, carefully, rather than fast.

**Documentation to read (in this order):**
1. Docker Compose overview — official docs, "Compose file reference" intro section only (don't read the whole reference yet)
2. Docker volumes — official docs "Manage data in Docker" page, just the "Volumes" section

**YouTube topics to search for (not specific videos — search these terms yourself and pick a well-reviewed short result):**
- "Docker Compose networks explained"
- "Docker volumes vs bind mounts"

**What NOT to worry about yet:** Kubernetes, Docker Swarm, multi-host networking, image building from scratch (you're using pre-built images today, not writing Dockerfiles). If a video starts talking about Kubernetes, close it — wrong day.

### 3. Building Tasks

**Task 1 — Initialize the repository**
- *Purpose:* every professional project starts with version control from commit zero, not "add git later."
- *Steps:* `mkdir ecommerce-lakehouse && cd ecommerce-lakehouse && git init`
- *Expected output:* an empty git repo with `.git/` present
- *Verify:* `git status` shows "no commits yet"
- *Common mistake:* initializing git *inside* a subfolder by accident (e.g., inside `ingestion/`) — always run `git init` at the project root, before creating any subfolders.
- *Estimated time:* 5 minutes

**Task 2 — Create the `.gitignore` before creating anything else**
- *Purpose:* if you create `.gitignore` after you've already created files, it's easy to accidentally commit secrets or generated data before it exists — do this first, as a habit.
- *Steps:* create `.gitignore` with entries for: `.env`, `data/`, `*.db`, `__pycache__/`, `.DS_Store`, `logs/`
- *Expected output:* a `.gitignore` file at the repo root
- *Verify:* open it and re-read each line, understand what each one prevents
- *Common mistake:* forgetting `.env` — this is the file that will hold your Kaggle and MinIO credentials starting Day 4, and it must never be committed.
- *Estimated time:* 5 minutes

**Task 3 — Create the full folder skeleton**
- *Purpose:* build the structure now, empty, so every future day has an obvious place to put files — this mirrors how you'd scaffold a new service at a real company before writing business logic.
- *Steps:* create these empty folders (with a `.gitkeep` placeholder file in each, since Git doesn't track empty folders): `ingestion/`, `spark_jobs/`, `checks/`, `dbt/`, `airflow/dags/`, `audit/`, `terraform/`, `docs/`, `.github/workflows/`
- *Expected output:* folder tree matching Section 6 of the Implementation Guide (skeleton only — files come later)
- *Verify:* `find . -type d -not -path './.git*'` shows all nine folders
- *Common mistake:* naming a folder differently than the guide (e.g., `spark/` instead of `spark_jobs/`) — small inconsistencies here cause confusing path errors weeks from now. Match the guide's Section 6 exactly.
- *Estimated time:* 10 minutes

**Task 4 — Write the README stub**
- *Purpose:* a README that says "coming soon" is still better than no README — GitHub visitors (and future-you) need to immediately understand what this repo will become.
- *Steps:* create `README.md` with: project title, one-paragraph description (behavioral analytics lakehouse, clickstream data, medallion architecture), a "Status: Under active development" line, and a placeholder "Architecture" heading you'll fill in with a diagram later.
- *Expected output:* `README.md` with these sections, even mostly empty
- *Verify:* read it out loud — does it make sense to someone who's never seen this project?
- *Common mistake:* writing the README as if it's already finished — be honest that it's in progress; recruiters actually appreciate visible, honest work-in-progress READMEs on personal projects.
- *Estimated time:* 15 minutes

**Task 5 — Install Docker Desktop and verify it**
- *Purpose:* confirm the tool actually works on your machine before you build anything on top of it.
- *Steps:* install Docker Desktop if not already installed; open Docker Desktop settings and set memory allocation to at least 8GB (Settings → Resources); run `docker run hello-world`
- *Expected output:* the `hello-world` container prints a success message
- *Verify:* `docker --version` and `docker compose version` both return version numbers
- *Common mistake:* leaving Docker Desktop's default RAM allocation (often 2GB) — this stack (Airflow + Spark + MinIO + Metabase together) will silently fail or hang with too little memory, and the error messages you'll get won't obviously point to "not enough RAM."
- *Estimated time:* 20–30 minutes (including install time)

**Task 6 — First commit**
- *Purpose:* lock in today's scaffolding as a clean, reviewable unit of work — this is also good git hygiene practice for how you'll work for the rest of the project.
- *Steps:* `git add .` then `git commit -m "chore: initialize repo structure and README stub"`
- *Expected output:* one commit in `git log`
- *Verify:* `git log --oneline` shows exactly one commit
- *Common mistake:* writing a vague commit message like "first commit" — practice writing commit messages that describe *what* changed, starting today.
- *Estimated time:* 5 minutes

### 4. Mentor Notes
- "We're building the full empty folder skeleton on Day 1, before any code exists, because it forces you to think about *where things belong* before you're under pressure to just get something working. This is exactly how you'd scaffold a new microservice on a real team."
- "Don't be tempted to skip the RAM allocation step. This single setting causes more mysterious failures in this exact stack than anything else in Phase 0 — I've seen engineers lose half a day debugging a Spark 'connection refused' error that was actually Docker running out of memory."
- "A vague README today is fine. A missing README is not. Recruiters and interviewers open the README first, always — even an honest 'in progress' README signals professionalism."

### 5. Definition of Done
- [ ] Git repo initialized with one clean commit
- [ ] `.gitignore` present and correct
- [ ] Nine-folder skeleton exists, matching Section 6 of the Implementation Guide
- [ ] `README.md` stub exists with title, description, status
- [ ] Docker Desktop installed, RAM allocation ≥ 8GB, `hello-world` runs successfully
- [ ] Screenshot: Docker Desktop's resource settings showing 8GB+ allocated (save to `docs/day1_docker_settings.png` — you won't need this in your final portfolio, but it's a useful personal record if something breaks later)

### 6. Validation
Run `docker run hello-world` — if it prints "Hello from Docker!", your engine is working. Run `git log --oneline` — you should see exactly one commit with a message describing the scaffolding. Open each of the nine folders and confirm each is genuinely empty (or has only a `.gitkeep`).

### 7. Common Beginner Mistakes
- Skipping the RAM allocation step because "it's probably fine" — it usually isn't, for this specific stack
- Naming folders inconsistently with the guide
- Committing before writing `.gitignore` (if you did this by accident, don't panic — just make sure nothing sensitive was committed; there's nothing sensitive yet on Day 1, but build the habit now)

### 8. Debugging Guide
If `docker run hello-world` fails:
- Check Docker Desktop is actually *running* (not just installed) — look for the whale icon in your system tray/menu bar
- On Mac/Windows, check virtualization is enabled in BIOS if Docker Desktop shows a virtualization error
- Run `docker info` — if this hangs or errors, Docker Desktop itself hasn't fully started yet; wait 30 seconds and retry

### 9. Learning Checkpoint
Before moving to Day 2, answer these out loud or in writing:
- What's the difference between a Docker image and a Docker container?
- Why does Docker Compose use a shared network between services by default, and why does that matter for Spark talking to MinIO later this week?
- Why did we create empty folders before writing any code?

If you can't answer the second question confidently, re-read the "Networks" concept above before Day 2 — it matters immediately.

### 10. Interview Preparation
- "Walk me through the difference between a Docker image and a container." — be ready to answer this in one sentence, not a paragraph.
- "Why would a team use Docker Compose instead of running containers manually?"

### 11. Git Workflow
- **Branch:** work directly on `main` for Phase 0 (environment setup isn't feature work yet — branching starts Day 4 onward once you're writing actual pipeline code)
- **Commit message today:** `chore: initialize repo structure and README stub`
- **Merge:** n/a today
- **Tag:** none yet

---

## DAY 2 — Docker Compose: Bringing Up the Stack

### 1. Daily Goal
Write a `docker-compose.yml` that brings up Airflow, Spark, MinIO, and Metabase together, and get all four healthy and able to reach each other by service name.

### 2. Learning First

**Concepts to understand first:**
- **Service definitions in Compose** — `image`, `ports`, `environment`, `volumes`, `depends_on` — you'll use all five today.
- **`depends_on` vs. actual readiness** — `depends_on` only waits for a container to *start*, not for the service inside it to be *ready* (e.g., MinIO's container can be "up" before MinIO itself can accept connections). This is a very common source of "it works if I restart, but not on first boot" bugs — you'll see this directly today.
- **Named volumes vs. bind mounts** — you'll bind-mount your local `airflow/dags/` folder into the Airflow container so Airflow can see DAG files you write on your own machine, and use named volumes for things like MinIO's internal data so it isn't tied to a specific host path.
- **Healthchecks** — a Compose `healthcheck` block lets a service formally report "I'm not just running, I'm actually ready," which you can then make other services wait on properly instead of relying on `depends_on` alone.

**Documentation to read:**
1. Docker Compose "Services top-level element" reference — skim for `depends_on`, `healthcheck`, `volumes`
2. MinIO's official Docker Compose quickstart page (just enough to understand the minimal service definition)
3. Apache Airflow's official "Running Airflow in Docker" quickstart (skim the docker-compose.yaml they provide — you'll adapt, not copy verbatim, since you're adding MinIO/Spark/Metabase alongside it)

**YouTube topics:**
- "Docker Compose healthcheck depends_on condition"
- "Airflow docker-compose quickstart" (to see the shape of a real Airflow Compose file before you adapt one)

**What NOT to worry about yet:** Spark-MinIO connectivity specifically (that's tomorrow, Day 3) — today just get all four containers *running and healthy*, even if they can't fully talk to each other yet.

### 3. Building Tasks

**Task 1 — Add the MinIO service**
- *Purpose:* this is your S3-compatible storage layer — everything else in the stack will eventually read/write through it.
- *Steps:* add a `minio` service using the official `minio/minio` image, exposing ports 9000 (API) and 9001 (console), with `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` set via environment variables (placeholder values for now — you'll move these to `.env` on Day 4), and a named volume for `/data`.
- *Expected output:* `minio` service defined in `docker-compose.yml`
- *Verify:* not yet — you'll bring the whole stack up together at the end of today
- *Common mistake:* forgetting the `command: server /data --console-address ":9001"` override — without it, MinIO's console won't be reachable on the port you expect.

**Task 2 — Add a MinIO healthcheck**
- *Purpose:* this is what solves the `depends_on`-isn't-enough problem from the Learning First section — other services should wait for MinIO to be *truly ready*, not just started.
- *Steps:* add a `healthcheck` block to the `minio` service that curls MinIO's `/minio/health/live` endpoint
- *Expected output:* a working healthcheck block
- *Verify:* after bringing the stack up, `docker ps` should show `(healthy)` next to the minio container, not just `Up`
- *Common mistake:* healthcheck syntax errors (missing `test:` as a list, wrong interval format) — Compose fails silently-ish here; always check `docker compose config` to validate your YAML before running it.

**Task 3 — Add the Spark service**
- *Purpose:* your compute engine for Bronze/Silver transforms, starting next week.
- *Steps:* add a `spark` service using `bitnami/spark`, set `SPARK_MODE=master`, expose 7077 (master) and 8081 (Spark UI)
- *Expected output:* `spark` service defined
- *Verify:* deferred to end of day

**Task 4 — Add the Airflow service(s)**
- *Purpose:* your orchestrator, starting Week 3.
- *Steps:* add `airflow-webserver` and `airflow-scheduler` services (or use Airflow's official multi-service Compose pattern), bind-mount `./airflow/dags` into the container's DAGs folder, expose port 8080
- *Expected output:* Airflow services defined
- *Verify:* deferred to end of day
- *Common mistake:* using `LocalExecutor` vs `CeleryExecutor` inconsistently — for this project, `LocalExecutor` is the right choice (simpler, sufficient for a single-machine portfolio project); don't reach for Celery, it adds a Redis dependency you don't need here.

**Task 5 — Add the Metabase service**
- *Purpose:* your BI layer, used starting Week 5+ for dashboards.
- *Steps:* add a `metabase` service using `metabase/metabase`, expose port 3000
- *Expected output:* `metabase` service defined
- *Verify:* deferred to end of day

**Task 6 — Validate the Compose file before running it**
- *Purpose:* catch YAML syntax errors before wasting time on a slow `docker compose up` that fails halfway through
- *Steps:* run `docker compose config`
- *Expected output:* a fully rendered, valid YAML dump with no errors
- *Verify:* the command exits 0 with no error text
- *Common mistake:* YAML indentation errors — Compose files are unforgiving about indentation; if `docker compose config` errors, the message usually tells you the exact line

**Task 7 — Bring the stack up**
- *Purpose:* the actual moment of truth for today.
- *Steps:* `docker compose up -d`, then `docker compose ps`
- *Expected output:* all four services show as `Up` (MinIO should show `healthy` specifically)
- *Verify:* `docker compose ps` output, plus visiting `http://localhost:9001` (MinIO console), `http://localhost:8080` (Airflow), `http://localhost:3000` (Metabase) in a browser — each should load *something*, even if not fully configured yet
- *Common mistake:* port conflicts if you have something else running locally on 8080/9000/3000 — if a port is taken, change the *host* side of the port mapping (e.g., `8081:8080`), not the container side

**Task 8 — Commit**
- *Steps:* `git add docker-compose.yml && git commit -m "feat: add docker-compose stack (Airflow, Spark, MinIO, Metabase)"`

### 4. Mentor Notes
- "The `depends_on` + `healthcheck` distinction you learned today isn't a Docker quirk you'll only see here — it's the same class of problem as service startup ordering in Kubernetes, or waiting for a database migration to finish before an app server accepts traffic. Understanding *why* 'started' isn't the same as 'ready' will save you real debugging time throughout your career."
- "Don't try to make Spark talk to MinIO today. Getting four containers healthy and reachable is enough for one day — tomorrow is entirely about the S3A connector, and trying to rush both into one day is how people burn a whole weekend on a debugging spiral."

### 5. Definition of Done
- [ ] `docker-compose.yml` defines all four services with correct ports and volumes
- [ ] MinIO has a working healthcheck
- [ ] `docker compose up -d` brings up all four services successfully
- [ ] All three web UIs (MinIO console, Airflow, Metabase) load in a browser
- [ ] Commit made
- [ ] Screenshot: `docker compose ps` output showing all services healthy/up → save to `docs/day2_stack_running.png`

### 6. Validation
`docker compose ps` — every service `Up`, MinIO `(healthy)`. Open all three URLs above in a browser tab each. If any fails to load after 60 seconds of the stack being up, treat that as a blocker — do not proceed to Day 3 with a broken service.

### 7. Common Beginner Mistakes
- Trying to fix Spark↔MinIO connectivity today (that's Day 3 — today is just "all containers healthy")
- Ignoring a non-healthy MinIO status and moving on anyway
- Committing real credentials in `docker-compose.yml` (today's are placeholders — real secrets move to `.env` tomorrow)

### 8. Debugging Guide
If a service won't start: `docker compose logs <service-name>` — read the last 20 lines, the actual error is almost always near the bottom. If MinIO never becomes healthy, check the healthcheck's curl command works *from inside the container*: `docker compose exec minio curl -f http://localhost:9000/minio/health/live`. If Airflow's webserver fails, it's very often a missing `AIRFLOW_UID` environment variable on Linux — check Airflow's official Compose docs for the fix.

### 9. Learning Checkpoint
- Why isn't `depends_on` alone enough to guarantee MinIO is ready when Spark starts?
- What's the difference between a bind mount and a named volume, and which did you use for Airflow's DAGs folder vs. MinIO's data?
- Why did we choose `LocalExecutor` over `CeleryExecutor` for this project?

### 10. Interview Preparation
- "What's the difference between `depends_on` and a healthcheck-gated startup in Docker Compose?"
- "When would you choose CeleryExecutor over LocalExecutor in Airflow?"

### 11. Git Workflow
- **Branch:** `main`
- **Commit message:** `feat: add docker-compose stack (Airflow, Spark, MinIO, Metabase)`
- **Merge:** n/a
- **Tag:** none yet

---

## DAY 3 — MinIO Buckets & Spark-to-MinIO Connectivity (Phase 0 Complete)

### 1. Daily Goal
Create the six MinIO buckets your architecture depends on, and prove Spark can actually read and write through the S3A connector — the single most important technical proof point of Phase 0.

### 2. Learning First

**Concepts to understand first:**
- **The S3A connector** — Spark doesn't natively speak "MinIO" or "S3"; it uses Hadoop's `S3AFileSystem` implementation, configured with an endpoint, access key, and secret key, which then makes `s3a://bucket/path` URIs work like any other filesystem path in Spark.
- **Why path-style vs. virtual-hosted-style access matters for MinIO** — real AWS S3 defaults to virtual-hosted-style URLs; MinIO (and S3A talking to MinIO) needs `path.style.access` enabled, or every request will fail with a confusing DNS-related error. This is the single most common first-time MinIO+Spark gotcha — expect to hit it today, and know that hitting it is normal, not a sign you did something wrong.
- **Buckets as your "raw/bronze/silver/gold" zones** — MinIO buckets are the direct analog to top-level S3 buckets or prefixes; in this project each medallion layer gets its own bucket, matching Section 6 of the Implementation Guide.

**Documentation to read:**
1. Hadoop-AWS module documentation, "S3A" section — just enough to recognize the config keys (`fs.s3a.endpoint`, `fs.s3a.access.key`, `fs.s3a.path.style.access`)
2. MinIO's "Spark with MinIO" example page or blog post (if available) for a working config reference

**YouTube topics:**
- "PySpark S3A MinIO configuration"
- "Hadoop AWS connector path style access"

**What NOT to worry about yet:** Delta Lake (that's Phase 2, next week) — today you're just proving plain read/write through S3A works at all, with a throwaway test file, before you build anything real on top of it.

### 3. Building Tasks

**Task 1 — Create the six buckets**
- *Purpose:* these are your raw/bronze/silver/gold/reference/logs zones — creating them explicitly (rather than letting code create them implicitly later) mirrors how infra is usually provisioned ahead of application code in real environments.
- *Steps:* via the MinIO console (`localhost:9001`) or the `mc` CLI, create buckets: `raw`, `bronze`, `silver`, `gold`, `reference`, `logs`
- *Expected output:* six buckets visible in the MinIO console
- *Verify:* console shows all six with 0 objects
- *Common mistake:* typos in bucket names that won't match what your code expects later (e.g., `bronz` instead of `bronze`) — copy the exact names from Section 6 of the Implementation Guide, don't retype from memory

**Task 2 — Move MinIO credentials into `.env`**
- *Purpose:* today is when secrets management actually starts to matter — get the habit right immediately.
- *Steps:* create `.env` (already gitignored from Day 1) with `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`; update `docker-compose.yml` to reference `${MINIO_ROOT_USER}` / `${MINIO_ROOT_PASSWORD}` instead of hardcoded values; create `.env.example` with placeholder values, which *is* committed, so anyone cloning the repo knows what variables they need
- *Expected output:* `.env` (gitignored) + `.env.example` (committed) both exist
- *Verify:* `git status` shows `.env.example` as trackable but `.env` does not appear at all
- *Common mistake:* accidentally committing `.env` because it was created before `.gitignore` picked it up — always run `git status` after creating any new file with secrets, as a reflex

**Task 3 — Restart the stack with the new env-based credentials**
- *Steps:* `docker compose down && docker compose up -d`
- *Expected output:* stack comes back up healthy with credentials now sourced from `.env`
- *Verify:* log into the MinIO console with the `.env` credentials

**Task 4 — Write a throwaway PySpark connectivity test**
- *Purpose:* prove the S3A connector works before you build any real transformation logic on top of it — isolating this proof from real logic makes debugging dramatically easier if something's wrong.
- *Steps:* using `docker exec` into the Spark container (or `spark-submit` from your host if you have PySpark installed locally for quick iteration), run a short interactive PySpark session that: sets `spark.hadoop.fs.s3a.endpoint`, `fs.s3a.access.key`, `fs.s3a.secret.key`, `fs.s3a.path.style.access=true`, `fs.s3a.connection.ssl.enabled=false`; writes a tiny DataFrame (e.g., 3 rows) to `s3a://raw/connectivity_test/`; reads it back
- *Expected output:* the read-back DataFrame matches what you wrote
- *Verify:* `.show()` on the read-back DataFrame prints the same 3 rows; also check the MinIO console — the `raw` bucket should now show a `connectivity_test/` prefix with Parquet part-files
- *Common mistake:* forgetting `path.style.access=true` — the classic first failure mode; if you get a DNS-lookup-style error or "unable to resolve bucket", this is almost certainly it

**Task 5 — Document the working Spark-MinIO config**
- *Purpose:* you'll reuse this exact config block in every Spark job starting next week — capture it now while it's fresh, rather than re-deriving it from memory later.
- *Steps:* save the working set of `spark.hadoop.fs.s3a.*` config keys (values pulled from env vars, not hardcoded) into a short `docs/design_decisions.md` note titled "Spark-MinIO Connectivity Config"
- *Expected output:* a short markdown note with the working config
- *Verify:* re-read it — could you copy-paste this into a new script cold and have it work?

**Task 6 — Clean up the test artifact, then commit**
- *Steps:* delete the `connectivity_test/` object from MinIO (via console or `mc rm`), then `git add . && git commit -m "feat: create MinIO zone buckets, move creds to .env, verify Spark S3A connectivity"`

### 4. Mentor Notes
- "This connectivity test is the single most valuable 30 minutes of Phase 0. Everything you build for the next six weeks assumes Spark can reliably read and write through MinIO — proving it in isolation today, with a throwaway 3-row DataFrame, means that if something breaks next week during a *real* Bronze job, you already know the S3A layer itself isn't the problem."
- "The `path.style.access` gotcha is not a sign you configured something wrong on a conceptual level — it's a genuinely common MinIO-specific quirk that catches experienced engineers too. Don't spiral on it; it's a one-line fix once you know to look for it."

### 5. Definition of Done
- [ ] Six MinIO buckets exist with exact names from Section 6 of the guide
- [ ] `.env` holds real MinIO credentials, gitignored; `.env.example` committed with placeholders
- [ ] Stack restarts cleanly using `.env`-sourced credentials
- [ ] PySpark connectivity test successfully writes and reads back through `s3a://`
- [ ] `docs/design_decisions.md` has the working Spark-MinIO config documented
- [ ] Test artifact cleaned up from MinIO
- [ ] Commit made
- [ ] **Phase 0 is now fully complete** — screenshot the six-bucket MinIO console view → `docs/day3_minio_buckets.png`

### 6. Validation
Re-run the connectivity test from a cold `docker compose down && docker compose up -d` — if it still works after a full restart, your config is genuinely durable, not just working by accident from leftover container state.

### 7. Common Beginner Mistakes
- Forgetting `path.style.access=true`
- Hardcoding credentials directly in the PySpark test script instead of reading from env — build the habit now, not later
- Not cleaning up test data, leading to confusion in Week 2 about what's "real" data in the raw bucket

### 8. Debugging Guide
DNS/bucket-resolution errors → check `path.style.access`. `Connection refused` → check the S3A endpoint matches the Docker service name (`minio`, not `localhost`, since Spark is running *inside* the Docker network). `403 Forbidden` → check `.env` credentials match what MinIO was actually started with (a stale container from before your `.env` change can cause this — try a full `docker compose down -v` and re-up if credentials seem stuck, note `-v` removes volumes so only do this since your buckets are still nearly empty).

### 9. Learning Checkpoint
- What does the S3A connector actually do, conceptually?
- Why does MinIO need `path.style.access=true` when real AWS S3 usually doesn't?
- Why did we write a throwaway connectivity test instead of just building the real Bronze job directly?

### 10. Interview Preparation
- "How does Spark talk to an S3-compatible object store? What's the S3A connector?"
- "Why is isolating infrastructure connectivity from business logic a good testing practice?"

### 11. Git Workflow
- **Branch:** `main`
- **Commit message:** `feat: create MinIO zone buckets, move creds to .env, verify Spark S3A connectivity`
- **Merge:** n/a
- **Tag:** `v0.1-phase0-complete` — tag today, since Phase 0 is now genuinely finished: `git tag v0.1-phase0-complete && git push --tags` (once you've set up a remote — if you haven't pushed to GitHub yet, do that now as part of today's wrap-up)

---

## DAY 4 — Kaggle API & Ingestion Design (Phase 1 begins)

### 1. Daily Goal
Get Kaggle API credentials working from environment variables, and design (on paper/markdown, not code yet) the exact behavior your ingestion script must have, so tomorrow's coding session has zero ambiguity.

### 2. Learning First

**Concepts to understand first:**
- **API authentication via environment variables vs. config files** — the Kaggle CLI/package traditionally reads a `kaggle.json` file; you'll instead source `KAGGLE_USERNAME`/`KAGGLE_KEY` from environment variables, which is the pattern that actually transfers to production secret managers (AWS Secrets Manager, Vault, etc. all ultimately inject env vars into a running process).
- **Idempotency, precisely defined** — "running this twice produces the same end state as running it once." This sounds obvious but has a specific, checkable meaning you'll implement tomorrow: if the target file already exists in MinIO and passes validation, the script must detect that and skip re-downloading, not just "not crash."
- **Exponential backoff** — retry delays that grow (e.g., 1s, 2s, 4s, 8s...) rather than retrying instantly in a tight loop, which is what actually gets you rate-limited or banned by real APIs, and is considered bad practice.
- **Structured logging vs. print statements** — structured logs are machine-parseable (JSON, with consistent fields like `event`, `status`, `duration_ms`) so that a log aggregator (or, in your case, your own eyes scanning `docker compose logs`) can filter and search reliably. `print("done")` cannot be queried; `{"event": "download_complete", "status": "success", "bytes": 48213911}` can.

**Documentation to read:**
1. Kaggle API official documentation — authentication section specifically
2. Python `logging` module docs — just the `JSONFormatter`-adjacent pattern (Python's stdlib doesn't ship JSON logging natively; you'll either hand-roll a simple JSON formatter or use `python-json-logger` — read enough to understand the concept before choosing)

**YouTube topics:**
- "Python exponential backoff retry pattern"
- "Structured logging Python JSON"

**What NOT to worry about yet:** writing the actual script — that's Day 5. Today produces a design document, a Kaggle account with API access, and nothing else code-wise.

### 3. Building Tasks

**Task 1 — Create a Kaggle account and generate an API token**
- *Purpose:* you need real credentials to test against a real API tomorrow.
- *Steps:* sign up at kaggle.com if you don't have an account; go to Account settings → API → "Create New API Token"; this downloads a `kaggle.json` containing your username and key
- *Expected output:* a `kaggle.json` file downloaded to your machine (do not commit this file — treat it as a secret)
- *Verify:* open it and confirm it contains a `username` and `key` field

**Task 2 — Move Kaggle credentials into `.env`**
- *Steps:* add `KAGGLE_USERNAME` and `KAGGLE_KEY` to your `.env` (pulling values from the downloaded `kaggle.json`); add corresponding placeholder entries to `.env.example`; delete or securely store the original `kaggle.json` outside the repo
- *Expected output:* `.env` has four total secrets now (2 MinIO, 2 Kaggle); `.env.example` documents all four keys with placeholder values
- *Verify:* `git status` still shows `.env` as untracked

**Task 3 — Confirm the target dataset exists and inspect its shape manually (browser only, not code)**
- *Purpose:* before automating a download, understand what you're actually downloading — file count, approximate size, column names — so you can write meaningful validation logic tomorrow.
- *Steps:* visit the `mkechinov/ecommerce-behavior-data-from-multi-category-store` Kaggle page in a browser; note the number of files, their approximate sizes, and skim the "Data" tab's column descriptions
- *Expected output:* a short note in `docs/design_decisions.md` listing the files you'll be pulling and their approximate sizes
- *Verify:* re-read your note — would it tell a teammate exactly what to expect from this dataset without them visiting Kaggle themselves?

**Task 4 — Write the ingestion service design doc**
- *Purpose:* this is today's real deliverable — a precise, checkable specification of tomorrow's script, so that tomorrow is implementation, not design-while-coding.
- *Steps:* in `docs/design_decisions.md`, write out, in plain language: the exact sequence of steps the script performs (auth → check-if-already-present → download → validate → extract → upload to MinIO → log); the exact idempotency check (what file/marker in MinIO means "already done"?); the exact retry policy (max attempts, backoff formula); the exact log events that must be emitted, with field names
- *Expected output:* a written spec detailed enough that tomorrow you're translating it into code, not inventing behavior as you go
- *Verify:* read the spec and ask "could someone else implement this from my notes alone?" If not, it's not detailed enough yet.
- *Common mistake:* being vague about the idempotency check specifically — "skip if already downloaded" is not specific enough; specify *exactly* what you'll check for (e.g., "does an object exist at `s3a://raw/ecommerce_events/source_file={name}/_SUCCESS`?")

**Task 5 — Install the `kaggle` Python package locally and test raw authentication**
- *Steps:* `pip install kaggle` (in a virtual environment — create one now if you haven't: `python -m venv venv && source venv/bin/activate`); run `kaggle datasets list -s ecommerce` from the terminal with `KAGGLE_USERNAME`/`KAGGLE_KEY` exported as env vars (not via the `kaggle.json` default location — you're proving env-var auth works, which is what your script will rely on)
- *Expected output:* a list of datasets prints without an authentication error
- *Verify:* the command succeeds and lists results
- *Common mistake:* leaving `kaggle.json` in the default `~/.kaggle/` location — the package will silently prefer that file over your env vars if both exist, masking a bug in your env-var-based auth; remove or rename that file to force env-var auth to actually be exercised

**Task 6 — Commit**
- *Steps:* `git add .env.example docs/design_decisions.md && git commit -m "docs: design ingestion service spec, add Kaggle env auth"`

### 4. Mentor Notes
- "Writing the design doc before the code isn't bureaucracy — it's the difference between debugging a vague idea and debugging a concrete spec. Tomorrow, when something doesn't work, you'll check it against what you wrote today, rather than trying to remember what you *meant* to build."
- "The `~/.kaggle/kaggle.json` gotcha in Task 5 is a good early lesson in a general principle: libraries often have multiple ways to find credentials, and if you don't know which one is actually being used, you don't actually know your auth path works — you just know *something* worked."

### 5. Definition of Done
- [ ] Kaggle account + API token created
- [ ] `.env` holds all 4 secrets (MinIO x2, Kaggle x2); `.env.example` documents all 4 with placeholders
- [ ] `docs/design_decisions.md` has: dataset shape notes, and a full ingestion service spec (sequence, idempotency check, retry policy, log events)
- [ ] `kaggle datasets list` succeeds using only env-var-based auth (no `~/.kaggle/kaggle.json` present)
- [ ] Python virtual environment created and `kaggle` package installed
- [ ] Commit made

### 6. Validation
Temporarily rename/move any `~/.kaggle/kaggle.json` out of the way, then re-run `kaggle datasets list -s ecommerce` with only env vars exported — success here proves your auth approach is real, not accidentally piggybacking on the default credentials file.

### 7. Common Beginner Mistakes
- Committing `kaggle.json` or its contents anywhere in the repo
- Writing a vague ingestion spec that doesn't specify the exact idempotency check
- Not testing auth in isolation before Day 5 — you want to know today that credentials work, not discover an auth bug while also debugging new script logic tomorrow

### 8. Debugging Guide
`401 Unauthorized` from the Kaggle CLI → double check `KAGGLE_KEY` was copied without extra whitespace, and that both env vars are actually exported in your current shell session (`echo $KAGGLE_USERNAME`). If it works with `kaggle.json` present but not with only env vars, the package version you installed may not support env-var auth the way you expect — check the `kaggle` package's changelog/docs for your installed version.

### 9. Learning Checkpoint
- Why is environment-variable-based auth more production-realistic than a checked-in credentials file?
- What exact condition will your script check to decide "this file is already ingested, skip it"?
- Why write the retry/backoff policy down before writing the retry code?

### 10. Interview Preparation
- "How do you handle API credentials in a script that needs to run unattended, e.g. on a schedule?"
- "What's exponential backoff, and why is naive immediate retrying considered bad practice against a third-party API?"

### 11. Git Workflow
- **Branch:** create and switch to `feature/ingestion-service` today — this is your first real feature branch, matching how you'll work through the rest of the project (one branch per phase/feature, merged to `main` once validated)
- **Commit message:** `docs: design ingestion service spec, add Kaggle env auth`
- **Merge:** not yet — stays open until ingestion is fully working (Day 7)
- **Tag:** none today

---

## DAY 5 — Building the Ingestion Script, Part 1: Auth, Download, Validation

### 1. Daily Goal
Write the first half of `ingestion/kaggle_ingest.py`: authentication, idempotency check, download, and validation — everything up to (but not including) the MinIO upload step.

### 2. Learning First

**Concepts to understand first:**
- **Checksum/size validation** — after downloading, how do you know the file isn't truncated or corrupted? Comparing downloaded byte size against what the source reports is a minimum bar; a true checksum (MD5/SHA) is stronger if the API provides one.
- **CLI argument design with `argparse`** — even a script you'll mostly run via Airflow later benefits from being independently runnable and testable from the command line during development.
- **Python logging configuration for JSON output** — configuring a `logging.Logger` with a custom `Formatter` that emits JSON lines, so today's script produces structured logs from the start rather than retrofitting them later.

**Documentation to read:**
1. Python `argparse` official tutorial — just enough for a couple of flags (e.g., `--force` to bypass the idempotency check for testing)
2. Kaggle API Python client reference — the `dataset_download_files` method specifically

**YouTube topics:**
- "Python argparse tutorial basics"
- "Python custom logging formatter JSON"

**What NOT to worry about yet:** the MinIO upload and retry/backoff logic — those are Day 6. Today's script, run standalone, should download and validate a file into a local temp directory only.

### 3. Building Tasks

Use the AI Agent Prompt from Section 11 of the Implementation Guide ("Kaggle ingestion") as your starting point with your AI coding assistant, but implement and understand it in these discrete steps rather than accepting one large generated file blindly:

**Task 1 — Set up the script skeleton with argparse and logging**
- *Purpose:* establish the script's entry point and structured logging before any real logic exists, so every subsequent task can just call `logger.info(...)` correctly from the start.
- *Expected output:* `ingestion/kaggle_ingest.py` runs with `python ingestion/kaggle_ingest.py --help` and shows usage
- *Verify:* running it with no real logic yet still logs a structured "script started" JSON line
- *Common mistake:* configuring logging inside a function that only runs conditionally — configure it once, at the top of `main()`, unconditionally

**Task 2 — Implement Kaggle authentication from env vars**
- *Purpose:* prove the auth path from Day 4 works *inside your actual script*, not just from a bare CLI command.
- *Expected output:* the script authenticates without needing `~/.kaggle/kaggle.json` present
- *Verify:* a structured log line confirms successful authentication (never log the actual key value — log only "authenticated: true/false")
- *Common mistake:* accidentally logging the raw `KAGGLE_KEY` value in a debug log line — treat this as a real security mistake to catch now, since production logging pipelines can leak secrets exactly this way

**Task 3 — Implement the idempotency check (against a local marker for now)**
- *Purpose:* build and test the "already done?" logic before MinIO is even in the picture, using a simple local marker file, so you can verify the *logic* in isolation before adding the MinIO dependency tomorrow.
- *Expected output:* a function that returns `True`/`False` for "should I skip this download"
- *Verify:* running the script twice — the second run logs "already ingested, skipping" and exits early
- *Common mistake:* checking for the *archive* file's existence as your marker instead of a definitive success marker — an interrupted download can leave a partial archive file that exists but is invalid; use a marker written only *after* successful validation

**Task 4 — Implement the download call**
- *Expected output:* the Kaggle dataset archive downloads to a local temp directory
- *Verify:* the file exists locally and its size roughly matches what you noted in yesterday's design doc
- *Common mistake:* downloading to a path inside your git repo without it being gitignored — double check `data/` (or wherever you're downloading to) is covered by `.gitignore` from Day 1

**Task 5 — Implement size-based validation**
- *Expected output:* a function that compares the downloaded file size against the Kaggle API's reported size for that dataset, logs the result, and raises a clear error if they don't match
- *Verify:* deliberately truncate a test file (e.g., `truncate -s 1000 somefile.zip`) and confirm your validation function correctly flags it as invalid
- *Common mistake:* not testing the *failure* path — it's easy to only test that valid files pass; make yourself watch it correctly reject a bad file too

**Task 6 — Write the success marker only after validation passes**
- *Expected output:* marker file created locally, containing at minimum a timestamp and the validated file's size
- *Verify:* re-run the full script — this time it should hit the idempotency check from Task 3 and skip immediately

**Task 7 — Commit**
- *Steps:* `git add ingestion/kaggle_ingest.py && git commit -m "feat: implement Kaggle auth, download, and size validation with local idempotency marker"`

### 4. Mentor Notes
- "Notice you built and tested idempotency against a *local* marker today, before MinIO enters the picture tomorrow. This is a deliberate sequencing choice: isolate the logic you're least confident about (idempotency correctness) from the infrastructure dependency (MinIO uploads) you already proved works on Day 3. When something breaks tomorrow, you'll know immediately whether it's the new MinIO-upload code or the idempotency logic, because the idempotency logic was already proven today."
- "The instinct to skip testing the failure path (Task 5) is extremely common and is exactly how truncated-file bugs make it to production. A validation function you've only ever seen succeed is not a validation function you've actually tested."

### 5. Definition of Done
- [ ] `ingestion/kaggle_ingest.py` exists with: argparse CLI, structured JSON logging, env-var Kaggle auth, local idempotency check, download, size validation, local success marker
- [ ] Script correctly skips on second run
- [ ] Deliberately corrupted/truncated file is correctly rejected by validation
- [ ] No secrets appear in any log line
- [ ] Commit made on `feature/ingestion-service`

### 6. Validation
Run the script three times in a row: first run downloads and validates; second run skips (idempotency); delete the local marker and truncate the downloaded file, then run a third time — validation should fail loudly with a clear error message, not a silent success.

### 7. Common Beginner Mistakes
- Logging secret values during auth debugging
- Using file *existence* instead of a post-validation marker for idempotency
- Not testing the validation failure path

### 8. Debugging Guide
If auth fails inside the script but worked from the bare CLI yesterday: check you're loading `.env` correctly inside the script (e.g., via `python-dotenv` — if you haven't installed it, do so now: `pip install python-dotenv`, and load it at the very top of the script before anything reads `os.environ`). If the download hangs, check your network/proxy settings aren't interfering, and add a timeout to the download call so a hang becomes a clear failure instead of an indefinite wait.

### 9. Learning Checkpoint
- Why did we test idempotency against a local marker before MinIO was involved?
- Why is "the archive file exists" a worse idempotency check than "a success marker exists"?
- What's one concrete way logging can accidentally leak secrets, and how did you avoid it today?

### 10. Interview Preparation
- "How would you design an idempotency check for a data ingestion job?"
- "Tell me about a time you deliberately tested a failure path, not just the happy path."

### 11. Git Workflow
- **Branch:** `feature/ingestion-service` (continued)
- **Commit message:** `feat: implement Kaggle auth, download, and size validation with local idempotency marker`
- **Merge:** not yet
- **Tag:** none today

---

## DAY 6 — Building the Ingestion Script, Part 2: MinIO Upload, Retry & Backoff

### 1. Daily Goal
Finish the ingestion script: extract the downloaded archive, upload the extracted files into the MinIO `raw` bucket at the correct partition path, move the idempotency marker into MinIO itself (not local disk), and add retry-with-backoff around the network-dependent steps.

### 2. Learning First

**Concepts to understand first:**
- **Why the idempotency marker must live in MinIO, not locally** — a local marker only protects you if the script always runs on the same machine with the same disk state. In production, ingestion jobs often run on ephemeral compute (a fresh container each run) — the *only* durable state is what's in your actual storage layer. Today you migrate yesterday's local-marker logic to check MinIO instead, which is the realistic version of this pattern.
- **`boto3`'s S3 client pointed at MinIO** — same idea as Spark's S3A connector from Day 3, but from plain Python: you configure `endpoint_url`, `aws_access_key_id`, `aws_secret_access_key` to point boto3 at MinIO instead of real AWS.
- **Retry with exponential backoff, implemented** — you understood the concept on Day 4; today you implement it, ideally using a small well-known library (e.g., `tenacity`) rather than hand-rolling a retry loop, since a battle-tested library correctly handles edge cases (jitter, max-attempt caps) that a first attempt at hand-rolling usually misses.

**Documentation to read:**
1. `boto3` S3 client docs — `upload_file`, `head_object` (you'll use `head_object` to check if a marker object exists, which raises a specific exception if it doesn't — read how to catch that exception correctly)
2. `tenacity` library docs — the `@retry` decorator with `wait_exponential` and `stop_after_attempt`

**YouTube topics:**
- "boto3 custom S3 endpoint MinIO"
- "Python tenacity retry decorator"

**What NOT to worry about yet:** wiring this into Airflow — that's Week 3. Today the script still runs standalone from the command line.

### 3. Building Tasks

**Task 1 — Configure a boto3 client pointed at MinIO**
- *Purpose:* your Python upload code needs the same "point at MinIO instead of AWS" treatment Spark got on Day 3.
- *Expected output:* a small helper function returning a configured `boto3.client('s3', endpoint_url=..., ...)` using MinIO credentials from `.env`
- *Verify:* a quick `s3_client.list_buckets()` call returns your six buckets from Day 3
- *Common mistake:* forgetting the MinIO endpoint needs `http://minio:9000` when run *inside* Docker but `http://localhost:9000` when run from your host machine during local development — make this configurable via an env var (e.g., `MINIO_ENDPOINT`) rather than hardcoding one or the other

**Task 2 — Implement archive extraction**
- *Expected output:* the downloaded archive is extracted to a local temp directory, listing the CSV file(s) inside
- *Verify:* extracted file(s) match the shape you noted in your Day 4 design doc

**Task 3 — Rewrite the idempotency check against MinIO**
- *Purpose:* replace yesterday's local-marker logic with a `head_object` check against a marker object in the `raw` bucket (e.g., `raw/ecommerce_events/source_file={name}/_SUCCESS`).
- *Expected output:* idempotency check now queries MinIO instead of local disk
- *Verify:* re-running the script with the marker already present in MinIO correctly skips, even after deleting all local temp files — this proves the state is durable, not accidentally still relying on local disk

**Task 4 — Implement the MinIO upload with the correct partition path**
- *Expected output:* extracted files uploaded to `s3a`-equivalent path `raw/ecommerce_events/source_file={filename}/ingested_date={YYYY-MM-DD}/` inside the `raw` bucket
- *Verify:* browse the MinIO console and confirm the exact path structure matches what Spark will expect to read on Day 8 (Phase 2 begins next week) — cross-check this now against Section 3 of the Implementation Guide, don't guess

**Task 5 — Wrap network calls (download, upload) with `tenacity` retry + exponential backoff**
- *Expected output:* download and upload calls are decorated with retry logic: max 5 attempts, exponential backoff
- *Verify:* deliberately break connectivity mid-run (e.g., disconnect wifi briefly, or point `MINIO_ENDPOINT` at a wrong port temporarily) and confirm the structured logs show multiple retry attempts with increasing delay before either succeeding or failing cleanly

**Task 6 — Write the success marker into MinIO only after upload fully succeeds**
- *Expected output:* an empty (or metadata-containing) `_SUCCESS` object written to the correct MinIO path
- *Verify:* Task 3's idempotency check now correctly detects this marker on a subsequent run

**Task 7 — Full end-to-end dry run and log review**
- *Steps:* delete the marker (`mc rm` or via console) to force a fresh run; run the full script start to finish; read through the entire structured log output line by line
- *Expected output:* a clean, readable sequence of JSON log lines telling the complete story of one run: auth → check → download → validate → extract → upload → marker written
- *Verify:* if you handed just the logs (not the code) to a colleague, could they tell you exactly what happened during this run?

**Task 8 — Commit and open a PR against `main`**
- *Steps:* `git add . && git commit -m "feat: complete MinIO upload with correct partitioning, retry/backoff, MinIO-based idempotency"`; push the branch; open a PR (even working solo, writing a real PR description is good practice — describe what changed and how you tested it)

### 4. Mentor Notes
- "Moving the idempotency marker from local disk to MinIO today isn't just a technical upgrade — it's the difference between a script that only works on your laptop and a script that would survive being run inside a fresh, throwaway container in a real Airflow deployment next week. Always ask yourself: 'if this ran somewhere with no memory of previous runs, would it still behave correctly?'"
- "Using `tenacity` instead of a hand-rolled `for attempt in range(5): try/except: time.sleep(2**attempt)` loop isn't laziness — a real retry library handles jitter (randomizing delay slightly so many clients retrying simultaneously don't all hammer the server at the exact same moment) and clean separation of 'which exceptions are worth retrying' from 'how many times.' Hand-rolling this well is more subtle than it looks."

### 5. Definition of Done
- [ ] `ingestion/kaggle_ingest.py` fully implements: auth, MinIO-based idempotency, download, validation, extraction, upload to the correct partitioned path, retry/backoff on network calls, MinIO-based success marker
- [ ] A deliberate connectivity interruption shows retry behavior in the logs, then either recovers or fails cleanly
- [ ] Re-running after a successful run correctly skips via the MinIO marker check
- [ ] Data visible in MinIO console at the exact expected partition path
- [ ] Commit made, branch pushed, PR opened
- [ ] Screenshot: MinIO console showing the uploaded partitioned data → `docs/day6_raw_zone_populated.png`

### 6. Validation
The strongest test today: delete all local temp files and the local venv's working directory state (simulate "fresh machine"), keep only `.env` and the code, then run the script once — it should behave identically to a first-ever run, proving no hidden local-state dependency remains.

### 7. Common Beginner Mistakes
- Leaving the MinIO endpoint hardcoded for one environment (inside-Docker vs. host-machine), breaking the other
- Writing the success marker before upload actually completes (should be strictly *after*)
- Not testing the retry path, only the happy path

### 8. Debugging Guide
`EndpointConnectionError` from boto3 → check `MINIO_ENDPOINT` matches whichever context you're running from (host vs. container). Uploads silently "succeeding" with 0 bytes → check you're reading extracted file contents correctly before calling `upload_file` (a common bug: extracting to the wrong directory and uploading an empty/wrong path). Retries never triggering during your deliberate failure test → check your `tenacity` decorator is actually wrapping the right function, and that the exception it's catching matches what boto3/requests actually raises on connection failure (log the exception type if unsure).

### 9. Learning Checkpoint
- Why must the idempotency marker live in durable storage (MinIO) rather than local disk for this to be production-realistic?
- What does exponential backoff with jitter protect against that plain exponential backoff doesn't?
- Walk through, from memory, the full sequence your script performs from start to finish.

### 10. Interview Preparation
- "Describe how you'd design an ingestion job to be safely re-runnable after a crash."
- "What's the difference between a script that's idempotent 'in theory' and one you've actually proven is idempotent?"

### 11. Git Workflow
- **Branch:** `feature/ingestion-service`
- **Commit message:** `feat: complete MinIO upload with correct partitioning, retry/backoff, MinIO-based idempotency`
- **Merge:** not yet — merge tomorrow after Day 7's final validation
- **Tag:** none today

---

## DAY 7 — End-to-End Ingestion Validation, Merge, and Week 1 Checkpoint

### 1. Daily Goal
Stress-test the ingestion service like an engineer reviewing someone else's PR would, merge it into `main`, update the README and portfolio artifacts, and formally close out Week 1.

### 2. Learning First

**Concepts to understand first:**
- **What "production-ready" actually means for a script this size** — not "handles every conceivable edge case," but: idempotent, observable (via logs), fails loudly and clearly rather than silently, and has no hidden local-state dependencies. Today you verify these properties explicitly, one at a time, rather than assuming they hold because the script "worked."
- **Why a self-review before merging matters even solo** — reading your own code as if reviewing a colleague's PR catches a different class of bug than writing/running it yourself; you're deliberately looking for "would I approve this" rather than "does this run."

**Documentation to read:** none new today — this is a consolidation day. Re-read your own `docs/design_decisions.md` entries from Days 4–6 and compare them against what you actually built; note and fix any drift.

**YouTube topics:** none required today.

**What NOT to worry about yet:** Bronze/Delta Lake — that starts Week 2, Day 8.

### 3. Building Tasks

**Task 1 — Full cold-start end-to-end test**
- *Purpose:* the definitive test of everything built this week.
- *Steps:* `docker compose down -v` (full reset, including volumes — this wipes MinIO's data, which is fine, you're testing from true zero) then `docker compose up -d`; wait for all services healthy; re-create the six buckets (Task 1 from Day 3 — you may want to script this now rather than doing it by hand a third time, worth a quick 10-minute addition); run the ingestion script fresh
- *Expected output:* the entire pipeline from an empty stack to populated `raw` bucket completes successfully with no manual intervention beyond starting the script
- *Verify:* MinIO console shows the correctly partitioned data; logs show a clean success sequence

**Task 2 — Self-review the ingestion script as if reviewing a colleague's PR**
- *Purpose:* catch issues a "does it run" test won't.
- *Steps:* read `kaggle_ingest.py` top to bottom and check: does every function have a clear single purpose? Is there any secret ever logged? Does every network call have retry logic? Is there dead code or leftover debug prints from earlier days?
- *Expected output:* a short list of cleanup items, then actually fix them
- *Verify:* the script reads cleanly start to finish with no leftover debugging artifacts

**Task 3 — Verify against the original design doc**
- *Steps:* re-read the ingestion spec you wrote on Day 4; check off each behavior against what you actually built; note any deliberate deviations and why
- *Expected output:* an updated note in `docs/design_decisions.md` reconciling spec vs. implementation
- *Verify:* no unexplained gaps between what you planned and what exists

**Task 4 — Merge the feature branch**
- *Steps:* ensure the PR description accurately reflects what was built; merge `feature/ingestion-service` into `main`; delete the feature branch
- *Expected output:* `main` now contains the fully working ingestion service
- *Verify:* `git log --oneline --graph` shows a clean merge history

**Task 5 — Tag the milestone**
- *Steps:* `git tag v0.2-phase1-complete && git push --tags`

**Task 6 — Update the README**
- *Purpose:* the README should always reflect current real progress, not lag behind — practice keeping it current weekly, starting now.
- *Steps:* add a "Progress" section listing Phase 0 and Phase 1 as complete; add the working `docker-compose up -d` + ingestion run instructions to a "Running Locally" section
- *Expected output:* README accurately describes what someone could actually clone and run today

**Task 7 — Take Week 1's portfolio screenshots**
- *Steps:* screenshot the MinIO console with real partitioned data in `raw`; screenshot a snippet of clean structured logs from a full successful run
- *Expected output:* both saved to `docs/`

### 4. Mentor Notes
- "The `docker compose down -v` cold-start test is the single best habit you can build this early in the project. Almost every 'works on my machine' bug hides behind leftover local state — if you make a full cold-start test a routine part of finishing any major piece of work, you'll catch a huge class of bugs before they ever become someone else's problem (or future-you's problem, three weeks from now, debugging why Bronze is reading stale test data)."
- "Keeping the README current *every week*, not just at the end, is what makes a GitHub repo look like an actively engineered project rather than a one-shot dump. Recruiters and interviewers do sometimes check commit history and README freshness — a README that visibly evolved alongside the code tells its own story."

### 5. Definition of Done
- [ ] Full cold-start (`down -v` → `up` → ingest) succeeds with zero manual fixes
- [ ] Self-review completed, cleanup items fixed
- [ ] Design doc reconciled with actual implementation
- [ ] `feature/ingestion-service` merged into `main`, branch deleted
- [ ] Tagged `v0.2-phase1-complete`
- [ ] README updated with accurate progress + run instructions
- [ ] Portfolio screenshots saved

### 6. Validation
Ask yourself honestly: if you deleted your local `venv`, `docker compose down -v`'d everything, and handed just the repo to another engineer with a valid `.env`, would they get a populated `raw` bucket by following only your README? If not, fix the README until the answer is yes — this is the real bar for "Definition of Done" on a portfolio project.

### 7. Common Beginner Mistakes
- Merging without actually re-testing cold-start first (testing only the state your dev environment happens to be in)
- Letting the README drift out of sync with reality
- Skipping the tag — tags are what let you (or an interviewer walking through your repo) find "the state of the project at the end of Week 1" precisely

### 8. Debugging Guide
If cold-start fails somewhere Day 1–3 already validated worked, that's a signal that some part of your setup was implicitly depending on state built up over the week (e.g., a bucket created manually once, never scripted) — this is exactly the kind of gap this exercise exists to surface; fix the root cause, don't just manually patch state and move on.

### 9. Learning Checkpoint — Full Week 1 Review
Answer all of these before moving to Week 2:
- Explain why we use MinIO instead of local folders, in your own words.
- What problem does an idempotency marker solve, and why does it need to live in durable storage?
- Walk through your ingestion script's full retry behavior on a network failure.
- What's the difference between `depends_on` and a healthcheck, and why did it matter for MinIO specifically?
- Why did we separate the design doc (Day 4) from implementation (Days 5–6)?

If any of these feel shaky, spend 30 minutes reviewing before Week 2 — Phase 2 (Delta Lake Bronze) assumes all of this is solid, working knowledge, not something you'd need to re-derive.

### 10. Interview Preparation — Week 1 Roundup
Be ready to give a 60-90 second answer to: *"Walk me through the ingestion layer of your data platform project."* Practice saying it out loud once, unscripted — this is genuinely good interview rehearsal.

### 11. Git Workflow
- **Branch:** `main` (post-merge)
- **Commit/merge:** `feature/ingestion-service` → `main`
- **Tag:** `v0.2-phase1-complete`

---

## 📦 END OF WEEK 1 — PORTFOLIO PROGRESS

**What to push to GitHub:** everything currently on `main` — Phase 0 and Phase 1 are both genuinely complete and cold-start-verified.

**Screenshots to have saved by now:**
- `docs/day2_stack_running.png` — all four services healthy
- `docs/day3_minio_buckets.png` — six buckets created
- `docs/day6_raw_zone_populated.png` — partitioned raw data in MinIO
- A clean structured-log excerpt from a successful ingestion run

**Architecture diagram:** not needed yet — hold off until at least Bronze exists (Week 2), so the diagram reflects real, working layers rather than aspirational ones.

**How the repository should look right now:** a clean `main` branch, one merged PR in history, two version tags (`v0.1-phase0-complete`, `v0.2-phase1-complete`), a README that accurately describes what's built and how to run it, and zero committed secrets.

**What recruiters would notice at this stage:** honestly, not much yet on its own — Week 1 is foundation work, and that's normal and expected. What *does* signal well even this early: clean commit history with meaningful messages, a README that's honest about being in-progress, and (if they look closely) the tagged milestones showing deliberate, staged progress rather than one giant dump commit.

---

# ✅ WEEK 1 CHECKPOINT

Before I generate Week 2, confirm the following are genuinely true — not "mostly true" or "I'll fix it later":

1. A full `docker compose down -v && docker compose up -d` brings the entire stack up healthy from zero, every time.
2. Running `ingestion/kaggle_ingest.py` from a clean state populates MinIO's `raw` bucket at the correct partitioned path, with clean structured logs.
3. Running it a second time correctly skips via the MinIO-based idempotency marker.
4. You can answer all five Day 7 checkpoint questions without looking anything up.
5. `main` is merged, tagged `v0.2-phase1-complete`, and pushed to GitHub with no secrets committed.

**Reply with confirmation (or tell me what's still broken) and I'll generate Week 2 — Delta Lake Bronze Layer.**
-e 

---


### Chapter 2 — Week 2: Delta Lake Bronze Layer & The Start of Incremental Silver

---

# WEEK 2 — Bronze Engineering: Schema, Lineage, ACID, and the First Steps of Incremental Design

**Week 2 maps to:** Implementation Guide Phase 2 (Bronze Layer with Delta Lake, Days 8–12) + the design/learning portion of Phase 3 (Incremental Silver, Days 13–14)
**Where we left off:** Phase 0 and Phase 1 are merged into `main`, tagged, and cold-start verified. Your `raw` bucket in MinIO holds real, correctly-partitioned clickstream data, ingested by a script you now trust because you proved its idempotency and retry behavior yourself.
**By the end of this week you will have:** a real Delta Lake Bronze table — the first layer of the project with actual ACID guarantees, lineage tracking, and Delta-native features (transaction log, time travel, schema evolution) — plus a fully specified design for how Silver's incremental MERGE logic will work, with the hardest part of that logic (deduplication) already built and tested in isolation.

---

## DAY 8 — Delta Lake Foundations & Schema Design

### 1. Daily Goal
Understand Delta Lake deeply enough to explain it without notes, then design (not yet write the full transform for) the explicit schema your Bronze layer will enforce on every incoming event.

### 2. Why Today's Work Matters
Everything from here through the end of the project sits on top of Delta Lake. If you treat it today as "Parquet with a fancier write command," you'll misunderstand why MERGE, time travel, and schema evolution work the way they do later — and you'll be debugging Silver's incremental logic in Week 3 without the mental model that makes it make sense. Today is conceptually the most important day since Day 3's Spark-MinIO connectivity proof.

### 3. Learning Objectives
- Explain what the Delta transaction log actually contains and why it's what makes ACID writes possible on top of plain object storage
- Explain time travel as a direct consequence of the transaction log, not a separate feature bolted on
- Design an explicit, strict schema for the clickstream event data, and explain why explicit schemas beat schema inference for a Bronze layer

### 4. Concepts to Understand First

**The Delta transaction log (`_delta_log/`)** — every write to a Delta table doesn't just add data files; it also appends a JSON entry to `_delta_log/` describing exactly which files are now "part of" the table's current version. Readers don't scan the storage folder and guess what's current — they read the log to know precisely which Parquet files constitute "the table" at a given version. This single mechanism is what gives you atomicity (a reader never sees a half-written version, because the log entry that makes new files "visible" is itself a single atomic file write) and time travel (old log entries are still there, so `VERSION AS OF 3` just means "use the file list from log entry 3," not "somehow reconstruct old data").

**Why this matters concretely for you:** on Day 2 you learned `depends_on` doesn't guarantee readiness. The transaction log solves a similar-shaped problem *for data*: without it, a reader could see a partially-written table mid-write (an incomplete version of "readiness"). Delta's log is the mechanism that makes "the write is either fully done or hasn't happened yet, from a reader's perspective" true.

**Schema enforcement vs. schema inference** — `spark.read.csv(..., inferSchema=True)` guesses types by sampling data, which is convenient for exploration and dangerous for a Bronze layer: a single malformed row (e.g., a price field that's occasionally "N/A" instead of a number) can silently change an entire column's inferred type, corrupting everything downstream. An explicit `StructType` schema, defined by you, means Spark either successfully casts incoming data to your declared types or flags rows that don't fit — which is exactly the "reject or quarantine" behavior your Bronze layer needs.

**Why Bronze enforces schema but doesn't reshape data** — Bronze's job is "land the data safely, with a known shape, and remember where it came from." It intentionally does *not* clean, deduplicate, or business-rule-filter anything yet — that's Silver's job (Week 3). Keeping this boundary sharp is a real production pattern: Bronze should always be re-derivable from Raw with zero business logic, so if a business rule turns out to be wrong later, you only have to reprocess Silver onward, not re-ingest.

### 5. Official Documentation to Read
1. Delta Lake official docs — "Table batch reads and writes" page, focus on the "Schema validation" and "How does Delta Lake manage feature compatibility" sections
2. Delta Lake docs — "Table utility commands," specifically `DESCRIBE HISTORY` (you'll use this Day 11, read it now so it's familiar)
3. Spark SQL docs — `StructType`/`StructField`/`DataType` reference, just enough to recognize the types you'll use today (`StringType`, `DoubleType`, `TimestampType`, `LongType`)

### 6. YouTube Topics to Study
- "Delta Lake transaction log explained"
- "Delta Lake time travel demo"
- "PySpark StructType explicit schema"

### 7. Building Tasks

**Task 1 — List every field in the raw dataset and its true type**
- *What:* open one of the extracted CSV files from your `raw` bucket (download it locally just for inspection, or use `head`) and list every column, its apparent type, and any values that look like edge cases (blank fields, unusual category_code formats)
- *Why:* you cannot write a correct explicit schema without first knowing, concretely, what the real data looks like — guessing this from the Kaggle page description alone (Day 4) is not enough
- *Why this design was chosen:* Bronze's schema should reflect *reality*, not an idealized version of the dataset — if `category_code` is sometimes null in the real data, your schema needs to allow that (nullable field) rather than fail on every row that lacks it
- *Expected output:* a short list in `docs/design_decisions.md` titled "Bronze Schema — Field Inventory," one line per column
- *Verify:* cross-check your list's column count against the Kaggle page's documented columns — they should match exactly
- *What can go wrong:* trusting the Kaggle page's column descriptions without looking at real rows — always verify against actual data
- *Debug it:* if a column's real values don't match what you expected, note it now rather than being surprised by a schema-casting failure on Day 9

**Task 2 — Design the explicit `StructType` schema (on paper/markdown first)**
- *What:* for each field from Task 1, decide the Spark type and nullability
- *Why:* deciding this deliberately, in writing, before touching code, means Day 9's implementation is translation, not design-under-pressure — same discipline as Day 4's ingestion spec
- *Expected output:* a markdown table in `docs/design_decisions.md`: column name → Spark type → nullable (Y/N) → reasoning for each
- *Verify:* for every field marked non-nullable, ask yourself "have I actually seen this field populated in 100% of sampled rows?" — if not, it should be nullable
- *Common mistake:* marking `user_session` non-nullable without checking — some rows in this dataset genuinely have edge-case session values; decide now whether "missing session" is a Bronze-schema violation (reject) or an acceptable-but-flagged value (allow, filter later in Silver) — the guide's Phase 3 filters null `user_session` in Silver, which implies Bronze should **allow** it through as nullable, and only Silver decides to drop it. This is an important distinction: Bronze enforces *shape*, Silver enforces *business validity*.

**Task 3 — Create `spark_jobs/schemas.py`**
- *What:* translate yesterday's design table into an actual `StructType` Python object
- *Why:* a single shared schema module means Bronze, and later any script that reads Bronze, references the same source of truth rather than redefining the schema in multiple places
- *Expected output:* `spark_jobs/schemas.py` exporting a `BRONZE_EVENT_SCHEMA` constant
- *Verify:* import it in a quick interactive PySpark shell and print it with `.simpleString()` — read it back and confirm it matches your Task 2 table exactly, field by field
- *Common mistake:* typos in field names that don't match the actual CSV header names — Spark's `StructType` with `header=True` matches by name, not position, by default in some read modes but not others; be explicit about which mode you're using and verify

**Task 4 — Set up a local experimentation notebook or scratch script**
- *What:* create `spark_jobs/_scratch_bronze_exploration.py` (prefixed with `_` to signal "not part of the pipeline, exploration only," and add this file to `.gitignore` since it's throwaway)
- *Why:* you're about to start writing real transformation code tomorrow — having a fast, disposable place to test schema-casting behavior against a small sample today, without touching the "real" pipeline script, mirrors how engineers actually explore data before committing to production code
- *Expected output:* a scratch script that reads a small sample of the raw CSV with your explicit schema applied and prints `.show()` and `.printSchema()`
- *Verify:* the printed schema matches your `BRONZE_EVENT_SCHEMA` exactly; `.show()` output looks sane (no obviously wrong casts, no unexpected nulls in fields you expected populated)

**Task 5 — Deliberately test a malformed row**
- *What:* hand-craft a tiny local CSV with one row that violates your schema (e.g., a non-numeric value in the `price` field) and read it with your explicit schema plus `mode="PERMISSIVE"` (Spark's default) so you can observe what actually happens to a bad row rather than assuming
- *Why:* you need to see this behavior with your own eyes before you build quarantine logic on Day 9 — "PERMISSIVE mode puts malformed rows in a `_corrupt_record` column if you include one in your schema" is something to verify, not just read about
- *Expected output:* a short note in `docs/design_decisions.md` describing exactly what you observed happening to the malformed row
- *Verify:* the malformed row is identifiable in the output (either as nulled-out fields or a `_corrupt_record` value, depending on how you configured the read)
- *Common mistake:* not including a `_corrupt_record`-style column and then being unable to actually find or count malformed rows — decide now how you'll surface them, since Day 9's quarantine logic depends on being able to identify them

**Task 6 — Commit**
- *Steps:* `git checkout -b feature/bronze-delta-layer && git add spark_jobs/schemas.py docs/design_decisions.md .gitignore && git commit -m "docs+feat: design and implement explicit Bronze schema, verify malformed-row handling"`

### 8. Definition of Done
- [ ] Field inventory of the real raw dataset documented
- [ ] Explicit schema design table in `docs/design_decisions.md`
- [ ] `spark_jobs/schemas.py` with `BRONZE_EVENT_SCHEMA` implemented and verified against the design table
- [ ] Scratch exploration script confirms schema casts real sample data correctly
- [ ] Malformed-row behavior observed and documented
- [ ] New feature branch created, committed

### 9. Validation Steps
Load a real (not hand-crafted) sample of your actual ingested data through `BRONZE_EVENT_SCHEMA` and confirm zero unexpected nulls in fields you marked non-nullable — if any appear, your Task 1 inventory missed an edge case, and you should revisit the schema before Day 9.

### 10. Common Beginner Mistakes
- Skipping the "look at real data first" step and designing a schema from memory of the Kaggle page
- Marking fields non-nullable optimistically instead of based on observed data
- Not deciding, explicitly, how malformed rows will be identifiable downstream

### 11. Debugging Guide
If `.printSchema()` shows types you didn't specify, you likely didn't pass your schema into the reader correctly (check you used `.schema(BRONZE_EVENT_SCHEMA)` before `.csv(...)`, not after, and that you didn't also pass `inferSchema=True`, which can silently override in some Spark versions). If every row shows as `_corrupt_record`, your schema's field order or names likely don't match the CSV header — double check with `header=True` behavior.

### 12. Mentor Notes
- "The distinction you nailed down today — Bronze enforces *shape*, Silver enforces *business validity* — is one of the most useful mental models in medallion architecture. Junior engineers often try to do all validation in one place. Keeping them separated is what lets you safely change a business rule next month without re-ingesting anything."
- "Notice that Task 5 today was about *watching* a system behave, not reading about how it's supposed to behave. This is a habit worth keeping for the rest of your career: verify assumptions about how a framework handles edge cases by testing them in isolation, especially before you build real logic on top of an assumption."

### 13. Industry Insights
- Real data platforms often maintain a **schema registry** (e.g., Confluent Schema Registry for Kafka-based systems) as the single source of truth for event shape, with producers required to register schema changes before shipping them — your `schemas.py` file is a simplified, file-based version of the same idea.
- "PERMISSIVE," "DROPMALFORMED," and "FAILFAST" are Spark's three CSV/JSON parse modes — knowing all three by name, and when you'd choose each, is a common interview detail question for anyone claiming Spark experience.

### 14. Git Workflow
- **Branch:** `feature/bronze-delta-layer` (new)
- **Commit message:** `docs+feat: design and implement explicit Bronze schema, verify malformed-row handling`
- **Merge:** not yet
- **Tag:** none today

### 15. README Updates
None required today — schema design is internal groundwork; the README's "Progress" section gets its Bronze update on Day 12, once Bronze is actually working end-to-end.

### 16. Portfolio Updates
None yet — today produces no visual artifact worth screenshotting. That starts Day 10.

### 17. Interview Questions
- "What is the Delta Lake transaction log, and how does it enable ACID guarantees on object storage?"
- "What's the difference between Spark's PERMISSIVE and FAILFAST CSV parsing modes, and when would you use each?"
- "Why might a Bronze layer intentionally avoid business logic, even simple filtering?"

### 18. Learning Checkpoint
- In your own words, what does the Delta transaction log actually store, and how does that give you time travel "for free"?
- Why did we decide `user_session` should be nullable in Bronze even though Silver will eventually drop rows with a null session?
- What did you observe happening to a malformed row under PERMISSIVE mode?

### 19. End-of-Day Checklist
- [ ] All 6 tasks complete
- [ ] Learning checkpoint answered confidently
- [ ] Feature branch created and pushed

### 20. Tomorrow's Preview
Day 9 turns today's schema design into the first real piece of `spark_jobs/bronze_transform.py`: reading raw data from MinIO with the enforced schema and separating clean rows from malformed ones — the quarantine logic Task 5 today set you up to build correctly.

---

## DAY 9 — Bronze Transform Part 1: Read, Enforce, Quarantine

### 1. Daily Goal
Write the first real functional piece of `spark_jobs/bronze_transform.py`: read raw data from MinIO through the schema built yesterday, and split it cleanly into "valid rows" and "quarantined rows," with the quarantined rows actually written somewhere you can inspect them.

### 2. Why Today's Work Matters
This is the first time real pipeline code — not exploration scripts — starts to exist. Everything from here through the end of the project follows the pattern you establish today: read → validate → separate good from bad → write, with visibility into what got rejected and why. Get this pattern right once, cleanly, and you'll reuse its shape in Silver next week almost unchanged.

### 3. Learning Objectives
- Implement schema-enforced reads from MinIO using the S3A config proven on Day 3
- Correctly identify and separate malformed rows using the behavior observed on Day 8
- Understand why "silently dropping bad rows" is a production anti-pattern, and build quarantine logic instead

### 4. Concepts to Understand First

**Quarantine, not silent drop** — a Bronze job that encounters a malformed row and simply excludes it from the output, with no record anywhere, means that six months from now, if someone asks "why is our event count lower than the source system reports," there's no way to answer. Writing malformed rows to a `bronze/_quarantine/` path (or similar) with the original raw values plus a reason preserves the ability to audit and, if needed, fix and reprocess them later.

**Reading through S3A with an explicit schema — combining two things you already know** — Day 3 proved Spark can read/write through MinIO. Day 8 proved your explicit schema correctly separates good and bad rows on local test data. Today is genuinely just combining those two already-proven pieces, which is why it should feel more like assembly than new invention — if it feels like you're learning something totally new today, that's a sign to go back and make sure Day 3 or Day 8's foundation is solid.

**Batch ID as a run identifier, introduced here, used everywhere from now on** — every Bronze run should be tagged with a unique `batch_id` (e.g., a UUID generated once per script invocation) so that every row written in a given run can be traced back to that specific run. This is the foundation of the lineage columns you'll fully wire in on Day 10, and it's also what will let Airflow (Week 3) pass a consistent identifier through a whole DAG run.

### 5. Official Documentation to Read
1. Spark docs — "DataFrame `columns` and filtering" (skim, just to have `filter`/`where`/`isNull` fresh)
2. Spark docs — CSV data source options page, specifically `columnNameOfCorruptRecord`

### 6. YouTube Topics to Study
- "PySpark handling corrupt records CSV"
- "PySpark filter isNull isNotNull"

### 7. Building Tasks

**Task 1 — Create `spark_jobs/bronze_transform.py` with a CLI entry point**
- *What:* build the script skeleton: `argparse` for a `--batch-id` argument (default to a generated UUID if not passed), structured logging setup matching the pattern from Day 5's ingestion script
- *Why:* consistency with your ingestion script's logging/CLI pattern means anyone reading both scripts recognizes the same shape immediately — this is what "house style" looks like on a real team
- *Expected output:* `python spark_jobs/bronze_transform.py --help` shows usage
- *Verify:* running with no real logic yet still logs a structured "job started" event including the batch_id being used
- *What can go wrong:* forgetting to make `--batch-id` optional with a sensible default — Airflow will pass one explicitly later, but you want the script independently runnable today

**Task 2 — Implement the schema-enforced read from MinIO**
- *What:* configure the Spark session with the S3A settings documented in `docs/design_decisions.md` from Day 3, then read the raw CSV path from the `raw` bucket using `BRONZE_EVENT_SCHEMA`, including a `columnNameOfCorruptRecord` column so malformed rows are identifiable
- *Why:* this is the direct continuation of Day 3's proof-of-connectivity and Day 8's schema design — nothing new conceptually, just real code now
- *Expected output:* a DataFrame containing both well-formed rows and rows with a populated corrupt-record column
- *Verify:* `.filter(col("_corrupt_record").isNotNull()).count()` returns a non-zero, plausible number (not zero — if it's zero on real data, double check you actually have some malformed rows to detect, or your test is meaningless; if it's suspiciously huge, like most of the dataset, something's wrong with your schema)
- *Common mistake:* reading with `mode="DROPMALFORMED"` by accident, which silently discards bad rows before you ever get the chance to quarantine them — explicitly use `PERMISSIVE` mode with the corrupt-record column, matching what you tested on Day 8

**Task 3 — Split into valid and quarantined DataFrames**
- *What:* `valid_df = raw_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")`; `quarantine_df = raw_df.filter(col("_corrupt_record").isNotNull())`
- *Why:* explicit separation, as two named DataFrames, makes the rest of the script's logic read clearly — anyone reviewing this code immediately understands there are two paths
- *Expected output:* two DataFrames whose row counts sum to the original raw row count
- *Verify:* `valid_df.count() + quarantine_df.count() == raw_df.count()` — write this as an actual assertion in the script during development (you can remove or downgrade it to a log-only check once you trust the logic, but verify it explicitly at least once)

**Task 4 — Write the quarantine DataFrame to MinIO**
- *What:* write `quarantine_df` to `s3a://bronze/_quarantine/batch_id={batch_id}/`, as plain Parquet (not Delta — quarantine data doesn't need ACID/versioning, it's a diagnostic artifact) including the original raw corrupt-record text
- *Why:* this makes malformed rows genuinely inspectable later, closing the loop from the "quarantine, don't silently drop" principle
- *Expected output:* quarantine data visible in the MinIO console under the bronze bucket's `_quarantine/` prefix
- *Verify:* open one of the written quarantine files and confirm you can see the original malformed row content, not just a generic "row rejected" flag

**Task 5 — Log row counts clearly**
- *What:* emit structured log lines for: total rows read, valid rows, quarantined rows, and the quarantine rate as a percentage
- *Why:* this single log line is what you'll glance at every single future Bronze run to sanity-check nothing has silently gone wrong — a sudden jump in quarantine rate is often the first visible symptom of an upstream schema change
- *Expected output:* a clear structured log entry with all four numbers
- *Verify:* the numbers are internally consistent (valid + quarantined = total)

**Task 6 — Run against your real ingested data and review results**
- *What:* run the script pointed at your actual `raw` bucket data from Week 1
- *Expected output:* a real quarantine rate for the real dataset — likely small (well under 1%) but very possibly non-zero
- *Verify:* read through several actual quarantined rows and confirm each genuinely violates the schema (not a false positive from an overly strict schema decision you made Day 8) — if you find false positives, that's valuable signal to revisit your schema, not a sign this task failed

**Task 7 — Commit**
- *Steps:* `git add spark_jobs/bronze_transform.py && git commit -m "feat: implement schema-enforced Bronze read with quarantine path for malformed rows"`

### 8. Definition of Done
- [ ] `bronze_transform.py` reads raw data from MinIO with the enforced schema
- [ ] Valid and quarantined rows are correctly separated
- [ ] Quarantined rows are written to MinIO in an inspectable form
- [ ] Row-count logging is clear and internally consistent
- [ ] Real ingested data has been run through the script and quarantine results manually reviewed
- [ ] Commit made

### 9. Validation Steps
Deliberately corrupt a copy of one raw CSV file locally (change a price field to text), upload it to a test path in `raw`, run the script against it, and confirm it lands in quarantine with the corrupted value visible — the same discipline as Day 5/6's "test the failure path, not just the happy path."

### 10. Common Beginner Mistakes
- Using `DROPMALFORMED` mode and losing visibility into bad rows entirely
- Writing quarantine data as Delta unnecessarily (adds complexity with no benefit for a diagnostic-only path)
- Not actually reading real quarantined rows to sanity-check the schema decisions from Day 8

### 11. Debugging Guide
If `_corrupt_record` never appears populated even against data you know is malformed: check the column is actually declared in your schema (Spark requires the corrupt-record column to be part of the schema you pass in, typically as a `StringType`, nullable). If row counts don't add up, check for an accidental `.cache()` or re-read between counting operations causing non-deterministic results against changing underlying data — unlikely here since your data is static, but worth ruling out.

### 12. Mentor Notes
- "The assertion in Task 3 — `valid + quarantine == total` — is a small thing, but it's the kind of self-checking code that separates 'I think this works' from 'I've proven this works.' Get comfortable adding cheap sanity assertions like this into pipeline code during development; you can always relax them to warnings once you trust the logic."
- "A near-zero but non-zero quarantine rate on real data is actually a good sign, not a problem — it means your schema is realistic (catching genuine edge cases) rather than either too loose (letting bad data through) or too strict (rejecting valid data). If you saw exactly zero quarantined rows on a multi-million-row real dataset, I'd actually ask you to double-check your schema isn't accidentally too permissive."

### 13. Industry Insights
- This read → validate → quarantine → log pattern is close to what's sometimes called a "dead letter queue" pattern in streaming systems (Kafka, SQS) — messages that fail processing go to a separate queue for inspection rather than blocking or silently vanishing. Recognizing this pattern by name across both batch and streaming contexts is a good signal in interviews.
- Companies with mature data platforms often set **alerting thresholds** on quarantine/rejection rates (e.g., page someone if the rejection rate exceeds 2x the trailing 7-day average) — you won't build real alerting in this project, but understanding why the rate itself is a meaningful operational metric (not just a debugging curiosity) is worth being able to explain.

### 14. Git Workflow
- **Branch:** `feature/bronze-delta-layer`
- **Commit message:** `feat: implement schema-enforced Bronze read with quarantine path for malformed rows`
- **Merge:** not yet
- **Tag:** none today

### 15. README Updates
None yet — Bronze isn't functionally complete until Delta writes exist (Day 10).

### 16. Portfolio Updates
None yet.

### 17. Interview Questions
- "How do you handle malformed records in a batch ingestion pipeline? Why not just drop them?"
- "What's a 'dead letter queue' pattern, and where have you applied a similar idea?"

### 18. Learning Checkpoint
- Why did we quarantine malformed rows to MinIO instead of just logging a count and discarding them?
- What would a sudden spike in your quarantine rate most likely indicate about the upstream data source?
- Why does quarantine data not need to be Delta format?

### 19. End-of-Day Checklist
- [ ] All 7 tasks complete
- [ ] Real quarantined rows manually reviewed and understood
- [ ] Committed on `feature/bronze-delta-layer`

### 20. Tomorrow's Preview
Day 10 adds the lineage metadata columns (`ingested_at`, `source_file`, `pipeline_run_id`, `batch_id`) to the valid rows and performs the actual **Delta** write — the moment this project's storage layer becomes meaningfully different from a plain-Parquet pipeline.

---

## DAY 10 — Bronze Transform Part 2: Lineage Metadata & the First Delta Write

### 1. Daily Goal
Add full lineage metadata to every valid row, and perform your first real Delta Lake write — then prove, using `DESCRIBE HISTORY`, that you actually get the ACID/versioning guarantees Day 8 explained conceptually.

### 2. Why Today's Work Matters
This is the day the theory from Day 8 becomes tangible. You'll write to a Delta table, look at its transaction log behavior directly, and confirm re-running the job produces a new version rather than corrupting the old one. This is also the day lineage metadata — a concept mentioned since the Implementation Guide's governance section — becomes real, queryable columns you'll rely on for the rest of the project.

### 3. Learning Objectives
- Implement lineage metadata columns correctly and understand why each one exists
- Perform a Delta Lake write and inspect its version history
- Understand what "append" means for a Delta table today, and why full MERGE-based incrementality is intentionally deferred to Silver

### 4. Concepts to Understand First

**Why Bronze appends rather than MERGEs (yet)** — Bronze's job, per Day 8's boundary discussion, is to land data safely with known shape and lineage. Since each ingestion run brings genuinely new source files (new `source_file` values), an **append** is the correct operation here — you're not updating existing rows, you're adding a new, clearly-lineage-tagged batch of them. True incremental MERGE logic (deciding whether a row is "new" vs. "an update to something already seen") is Silver's job, starting Day 13, because that's where deduplication and business-key logic actually live. Don't try to build MERGE logic into Bronze today — it doesn't belong there yet.

**The four lineage columns, and why each exists specifically:**
- `ingested_at` — when this row was written to Bronze (not when the event happened — that's `event_time`, already in the data). Distinguishing "when did the thing happen" from "when did our pipeline process it" is a distinction real analytics teams rely on constantly (this is sometimes called the difference between event time and processing time).
- `source_file` — which raw file this row came from, letting you trace a row back to a specific ingestion run's output from Week 1.
- `pipeline_run_id` — a value that would eventually be Airflow's DAG run ID (for now, a UUID you generate); ties every row written in one execution to that execution.
- `batch_id` — from Day 9's CLI argument; in this project it often equals `pipeline_run_id` conceptually, but keeping them as separate columns leaves room for a single pipeline run to process multiple logical batches later without redesigning the schema.

**`DESCRIBE HISTORY` and what it proves** — running `DESCRIBE HISTORY delta.`bronze/ecommerce_events`` after two separate write runs should show two version entries, each with an operation type (`WRITE`), timestamp, and metrics (rows written). This is your concrete, hands-on proof that Delta's transaction log from Day 8's reading is real and working, not just documentation you took on faith.

### 5. Official Documentation to Read
1. Delta Lake docs — "Write to a table," specifically the `append` mode section
2. Delta Lake docs — `DESCRIBE HISTORY` command reference

### 6. YouTube Topics to Study
- "Delta Lake DESCRIBE HISTORY practical example"
- "Delta Lake append vs overwrite mode"

### 7. Building Tasks

**Task 1 — Add lineage columns to the valid DataFrame**
- *What:* using `withColumn`, add `ingested_at` (current timestamp), `source_file` (derived from the input file path using Spark's `input_file_name()` function), `pipeline_run_id` and `batch_id` (from the CLI arg / generated UUID)
- *Why:* exactly as described above — these four columns are what let you answer "where did this row come from and when" for the rest of the project's life
- *Expected output:* `valid_df` now has 4 additional columns beyond the original schema
- *Verify:* `.show()` a few rows and confirm all four lineage columns are populated sensibly, with `source_file` showing a real, specific file path (not a generic placeholder)
- *Common mistake:* using Python's `datetime.now()` captured once outside the DataFrame transformation instead of Spark's `current_timestamp()` function — the former gets baked in as a literal at DataFrame *definition* time in a way that can behave unexpectedly with Spark's lazy evaluation; use Spark-native functions for anything that should reflect actual write time

**Task 2 — Write `valid_df` as a Delta table, partitioned by `event_date`**
- *What:* derive an `event_date` column from `event_time` (a simple date-truncation), then write using `.format("delta").mode("append").partitionBy("event_date")` to `s3a://bronze/ecommerce_events/`
- *Why:* partitioning by date is the standard pattern for time-series-like event data — nearly every downstream query (Silver, dbt marts) will filter by date range, and partitioning lets Spark skip reading irrelevant files entirely rather than scanning everything
- *Expected output:* a Delta table now exists at that path, with data organized into date-based partition folders
- *Verify:* browse the MinIO console — you should see both actual Parquet data files organized under `event_date=YYYY-MM-DD/` folders, and a `_delta_log/` folder at the table root
- *Common mistake:* partitioning by a high-cardinality column (like `user_id`) instead of `event_date` — this would create an enormous number of tiny partition folders, hurting performance rather than helping it; this is a common "more partitioning is always better" misconception worth explicitly avoiding

**Task 3 — Run `DESCRIBE HISTORY` and inspect the result**
- *What:* in a PySpark shell (or a small script), run `spark.sql("DESCRIBE HISTORY delta.`s3a://bronze/ecommerce_events`").show(truncate=False)`
- *Why:* this is your hands-on proof of Day 8's conceptual learning
- *Expected output:* one row showing version 0, operation `WRITE`, with `operationMetrics` showing rows written matching your logged valid-row count from Day 9's logging pattern
- *Verify:* the row count in `operationMetrics` matches what your script's own logging reported — if these disagree, something's wrong with either your logging or your understanding of what got written

**Task 4 — Run the job a second time and observe versioning**
- *What:* re-run `bronze_transform.py` with a new `--batch-id` against the same source data (simulating a second ingestion of the same files, which shouldn't normally happen given Week 1's idempotency, but is a useful test here)
- *Why:* prove that Delta versions rather than corrupts on repeated writes
- *Expected output:* `DESCRIBE HISTORY` now shows two versions; total row count in the table has roughly doubled (since this was an append of the same data again, not deduplicated — that's expected and correct, since Bronze doesn't deduplicate, only Silver does)
- *Verify:* query `SELECT COUNT(*) FROM delta.`s3a://bronze/ecommerce_events`` at version 0 using `VERSION AS OF 0` and compare it against the current (version 1) count — version 0's count should be unaffected by the second write, proving time travel works
- *Common mistake:* being confused/alarmed that row counts "doubled" — this is expected and correct given today's scope (append-only, no dedup yet); resist the urge to add deduplication logic here, that's explicitly Silver's job and you'll build it properly on Day 14

**Task 5 — Query using time travel**
- *What:* run a query against `VERSION AS OF 0` and separately against the current version, and print both row counts side by side
- *Expected output:* two different, correct counts, proving you can query historical versions of the table on demand
- *Verify:* the version-0 count matches exactly what Day 9's logging reported for that specific run

**Task 6 — Update the connectivity config note from Day 3**
- *What:* add a short addendum to your `docs/design_decisions.md` Spark-MinIO config note, capturing the additional Delta-specific Spark packages/config needed (e.g., `spark.sql.extensions` for Delta, the Delta Maven coordinate) beyond the plain S3A config from Day 3
- *Why:* this is exactly the kind of "future you, or a teammate, needs this" documentation that pays off the next time you spin up a new script that needs Delta+MinIO together

**Task 7 — Commit**
- *Steps:* `git add spark_jobs/bronze_transform.py docs/design_decisions.md && git commit -m "feat: add lineage metadata columns, perform first Delta Lake append write, verify version history and time travel"`

### 8. Definition of Done
- [ ] All four lineage columns implemented correctly
- [ ] Valid data written as a partitioned Delta table to `bronze/ecommerce_events`
- [ ] `DESCRIBE HISTORY` shows accurate version entries matching logged row counts
- [ ] Time travel query (`VERSION AS OF 0`) proven to work and return a different, correct result than the current version
- [ ] Delta-specific config documented
- [ ] Commit made
- [ ] Screenshot: MinIO console showing the `_delta_log/` folder alongside partitioned data files → `docs/day10_delta_table_structure.png`

### 9. Validation Steps
Run `DESCRIBE HISTORY` a third time after a third run with a new batch ID — confirm three clean version entries exist, each with correct, non-overlapping metrics, and that early versions remain queryable via `VERSION AS OF` throughout.

### 10. Common Beginner Mistakes
- Trying to prevent "duplicate" rows from the repeated test run in Task 4 — that's explicitly out of scope for Bronze
- Partitioning by a high-cardinality column
- Capturing Python `datetime.now()` outside Spark's lazy evaluation instead of using `current_timestamp()`

### 11. Debugging Guide
If `DESCRIBE HISTORY` fails with a "not a Delta table" error, double check you're using `.format("delta")` on write (not the default Parquet) and that your Spark session has Delta's SQL extensions and catalog configured (`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`, `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`) — a very common first-Delta-table mistake is writing Delta files correctly but querying them with a Spark session that isn't Delta-aware.

### 12. Mentor Notes
- "Today is the day this project stops being 'a pipeline that happens to use Delta' and starts being 'a pipeline that relies on Delta's actual guarantees.' Notice how little new code Task 3–5 required — proving time travel works is almost entirely about querying, not writing. That's the point of ACID guarantees: once you write correctly, a lot of powerful behavior comes for free."
- "The instinct to 'fix' the doubled row count in Task 4 is exactly the instinct to watch for and resist. A junior engineer who doesn't yet trust the Bronze/Silver boundary often starts adding cleanup logic everywhere, 'just in case.' A senior engineer trusts the boundary they designed and fixes the actual problem (deduplication) in the actual right place (Silver, next week)."

### 13. Industry Insights
- Delta Lake's time-travel feature is genuinely used in production incident response — "what did this table look like right before the bad deploy at 2pm" is a real question `VERSION AS OF`/`TIMESTAMP AS OF` answers directly, without needing a separate backup system.
- Partitioning strategy (date-based vs. otherwise) is a frequent system-design interview topic for data engineering roles — being able to explain *why* you chose `event_date` and *why not* a higher-cardinality column shows real judgment, not just tool familiarity.

### 14. Git Workflow
- **Branch:** `feature/bronze-delta-layer`
- **Commit message:** `feat: add lineage metadata columns, perform first Delta Lake append write, verify version history and time travel`
- **Merge:** not yet
- **Tag:** none today

### 15. README Updates
None yet — one more day before Bronze + its Soda gate is fully complete.

### 16. Portfolio Updates
Screenshot saved today (Task 6/Definition of Done) — hold off on posting anything publicly until Day 12's full validation.

### 17. Interview Questions
- "Walk me through what `DESCRIBE HISTORY` on a Delta table actually shows you, and why that's useful operationally."
- "Why did you partition this table by date rather than by user ID?"
- "What's the difference between event time and processing time, and why do you track both?"

### 18. Learning Checkpoint
- Why is Bronze an append-only operation in this project, while Silver will use MERGE?
- What four lineage columns did you add today, and what specific question does each one answer?
- What did the time-travel query in Task 5 actually prove, beyond "the query ran without error"?

### 19. End-of-Day Checklist
- [ ] All 7 tasks complete
- [ ] Time travel demonstrated with two genuinely different, correct results
- [ ] Screenshot saved
- [ ] Committed

### 20. Tomorrow's Preview
Day 11 closes the loop on Phase 2 by wiring in the first real Soda Core data quality gate against Bronze — schema conformance, null checks, and freshness — run manually for now (Airflow integration is Week 3), proving the quality-gating concept end-to-end before it's automated.

---

## DAY 11 — Soda Core: The First Data Quality Gate

### 1. Daily Goal
Install Soda Core, write `checks/bronze_checks.yml`, and run a real scan against your Bronze Delta table — proving, with a deliberately injected bad batch, that a quality failure is actually detectable and would actually stop a real pipeline.

### 2. Why Today's Work Matters
Section 0 of the Implementation Guide identified "dbt tests only, applied too late" as one of v1.0's core weaknesses. Today is where that architectural decision becomes real, working code — your first quality gate, positioned exactly where the guide specifies: immediately after Bronze, not buried at the end of the pipeline.

### 3. Learning Objectives
- Understand Soda Core's YAML check syntax and how it maps to the check categories from the Implementation Guide (schema, nulls, duplicates, accepted values, freshness, volume)
- Run a scan against a real Delta table and interpret its output
- Prove, with an injected failure, that a check genuinely catches a real problem — not just that the YAML is syntactically valid

### 4. Concepts to Understand First

**Why Soda Core, revisited with hands-on context** — you read the reasoning in the Implementation Guide (Section 1): lighter weight than Great Expectations, YAML checks that read naturally, first-class Airflow integration. Today you'll feel the "reads naturally" part directly — a Soda check like `missing_count(user_session) = 0` is close to plain English, which matters when you're the one maintaining these checks solo.

**What a "freshness" check actually measures** — a freshness check compares the most recent timestamp in a column (here, `event_time` or `ingested_at`) against the current time, failing if the gap exceeds a threshold. This check exists to catch a *silently stopped* pipeline — one that isn't erroring, just quietly not running anymore — which is a failure mode that row-count and null checks alone won't catch.

**Why a quality check must be able to fail the *process*, not just print a report** — a report nobody reads because the pipeline "succeeded anyway" provides zero real protection. Today you're testing Soda Core's exit code behavior specifically, because that exit code is what will let Airflow (Week 3) actually halt a DAG on failure, per Section 7 of the Implementation Guide.

### 5. Official Documentation to Read
1. Soda Core official quickstart — installation and first scan
2. Soda Core "Checks" reference — read the sections on: `schema`, `missing_count`, `duplicate_count`, `valid values`, `freshness`

### 6. YouTube Topics to Study
- "Soda Core data quality checks YAML tutorial"
- "Soda Core Airflow integration" (skim only — full implementation is Week 3, but seeing the shape now helps today's design)

### 7. Building Tasks

**Task 1 — Install Soda Core and configure its Delta/DuckDB connection**
- *What:* `pip install soda-core-duckdb` (Soda Core connects to Bronze here via a DuckDB connection reading the Delta table's underlying Parquet files, since Soda Core doesn't have a native "Delta on MinIO" data source out of the box — DuckDB can read Delta tables directly via its `delta` extension); create `checks/configuration.yml` with the connection details
- *Why:* this is a realistic constraint you're navigating exactly like you would with any tool that doesn't have first-class support for a specific storage format — find the closest supported path (DuckDB's Delta reading capability) rather than assuming every tool combination works out of the box
- *Expected output:* `checks/configuration.yml` exists with a working DuckDB-based connection to your Bronze table
- *Verify:* a minimal Soda scan with zero real checks (just confirming connectivity) runs without a connection error
- *Common mistake:* assuming Soda Core has native Delta Lake support without checking — always verify a tool's actual supported data sources before designing around an assumption

**Task 2 — Write the schema conformance check**
- *What:* in `checks/bronze_checks.yml`, add a `schema:` check asserting the expected columns and types exist
- *Why:* this is the automated enforcement of the exact schema you designed by hand on Day 8 — today it becomes a check that runs every time, not something you verify manually
- *Expected output:* a passing schema check against your real Bronze table
- *Verify:* run the scan, confirm this specific check shows as passed in the output

**Task 3 — Write null checks on required fields**
- *What:* add `missing_count(event_time) = 0` and `missing_count(product_id) = 0` (fields that should genuinely never be null, per your Day 8 schema decisions)
- *Expected output:* both checks pass against real data
- *Verify:* scan output confirms pass

**Task 4 — Write the freshness check**
- *What:* add a freshness check on `ingested_at` with a threshold appropriate for this project (e.g., fail if the most recent `ingested_at` is more than 2 days old) — for a portfolio project run manually/on-demand rather than daily-scheduled yet, choose a threshold that makes sense given your actual run cadence, and document your reasoning
- *Expected output:* passes right after you've just run Bronze
- *Verify:* scan output confirms pass; note in `docs/design_decisions.md` why you chose this specific threshold

**Task 5 — Write a volume anomaly check**
- *What:* add a check comparing today's row count to a reasonable expectation — since you don't yet have multiple days of real trailing history (that comes naturally once Airflow runs this daily in Week 3), implement this as a simple `row_count > 0` for now with a clear comment noting it should evolve into a true trailing-average comparison once daily runs accumulate history
- *Why:* it's honest engineering to implement the achievable version of a check now and document the intended evolution, rather than faking a trailing-average check against data you don't actually have yet
- *Expected output:* passing check with a clear code comment explaining the current simplification

**Task 6 — Deliberately break something and confirm the check catches it**
- *What:* using your scratch script pattern from Day 8, write a small batch of intentionally bad rows (nulls in `event_time`) directly into a *test copy* of the Bronze table (do not corrupt your real Bronze table — write to a separate test path), then run the Soda scan against that test path
- *Why:* exactly the same "test the failure path" discipline from Day 5/9 — a check you've only seen pass is not a check you've verified
- *Expected output:* the null check fails, and the scan process exits with a non-zero exit code
- *Verify:* explicitly check `echo $?` after the scan run — confirm it's non-zero on the failing case and zero on the passing case; this exit code is exactly what Airflow will key off of in Week 3

**Task 7 — Commit**
- *Steps:* `git add checks/ && git commit -m "feat: implement Soda Core Bronze quality checks (schema, nulls, freshness, volume), verify failure exit codes"`

### 8. Definition of Done
- [ ] Soda Core installed and connected to Bronze data
- [ ] `checks/bronze_checks.yml` implements schema, null, freshness, and volume checks
- [ ] All checks pass against real Bronze data
- [ ] A deliberately injected bad batch causes the relevant check to fail with a non-zero exit code
- [ ] Freshness threshold and volume-check simplification both documented with reasoning
- [ ] Commit made

### 9. Validation Steps
Run the full scan twice in a row against real data — confirm consistent pass results (not flaky), then run once more against the deliberately-corrupted test path to reconfirm the failure and non-zero exit code are both reproducible, not a one-off fluke.

### 10. Common Beginner Mistakes
- Only ever running Soda checks against known-good data, never verifying the failure path
- Not checking the actual process exit code, only reading the printed report visually
- Faking a volume-anomaly check against a single day of data instead of honestly scoping it to what's achievable right now

### 11. Debugging Guide
If Soda Core can't read the Delta table at all, confirm DuckDB's `delta` extension is installed and loadable (`INSTALL delta; LOAD delta;` in a DuckDB session) before troubleshooting Soda itself — isolate the DuckDB-reads-Delta capability from the Soda-runs-checks capability, the same "prove the underlying connectivity first" discipline as Day 3. If a check that should fail doesn't, double check you actually wrote the bad test data to the path the check is scanning, not accidentally back to your real Bronze table.

### 12. Mentor Notes
- "The exit-code check in Task 6 might feel like a small technical detail, but it's the entire reason this phase of the project exists. A quality framework that produces a report nobody automatically acts on isn't actually gating anything — it's just documentation. The non-zero exit code is what turns a report into a gate, and confirming it today means Week 3's Airflow integration is genuinely just 'wire up something already proven to work,' not 'discover for the first time that this doesn't halt anything.'"
- "Notice the honest scoping decision in Task 5 — implementing the achievable version of a check and documenting its planned evolution, rather than faking a more sophisticated check against data you don't have yet. This kind of honesty about current limitations, documented clearly, is exactly what separates a portfolio project that reads as genuinely engineered from one that reads as performing sophistication it doesn't have."

### 13. Industry Insights
- Many real data platforms extend exactly this pattern — quality-check exit codes gating pipeline progression — using tools like Great Expectations' "checkpoint" actions or dbt's `--fail-fast` flag; the specific tool changes, the "quality gate produces an exit code that orchestration keys off of" pattern doesn't.
- "How do you decide quality check thresholds (like your freshness window)?" is a common follow-up question in DE interviews — having a real, reasoned answer ("I chose 2 days because our run cadence is roughly daily and I wanted a buffer for weekend gaps," or similar honest reasoning) is far stronger than an arbitrary number.

### 14. Git Workflow
- **Branch:** `feature/bronze-delta-layer`
- **Commit message:** `feat: implement Soda Core Bronze quality checks (schema, nulls, freshness, volume), verify failure exit codes`
- **Merge:** not yet — merges tomorrow after Day 12's full Phase 2 validation
- **Tag:** none today

### 15. README Updates
None yet — tomorrow.

### 16. Portfolio Updates
None yet — tomorrow.

### 17. Interview Questions
- "How does a data quality check actually stop a bad pipeline run, mechanically? What does the orchestrator key off of?"
- "How do you decide on quality check thresholds like a freshness window?"
- "Tell me about a time you deliberately scoped down a check to what was honestly achievable rather than faking sophistication."

### 18. Learning Checkpoint
- Why does a quality check need to produce a distinguishable failure exit code, not just a printed report?
- What specific silent-failure mode does a freshness check catch that a simple row-count check wouldn't?
- Why did you choose to simplify the volume check for now, and what would you need before implementing the "real" trailing-average version?

### 19. End-of-Day Checklist
- [ ] All 7 tasks complete
- [ ] Failure path explicitly tested with a non-zero exit code confirmed
- [ ] Committed

### 20. Tomorrow's Preview
Day 12 closes out Phase 2 entirely: a full cold-start validation of ingestion → Bronze → Soda gate together, a self-review, merge to `main`, README update, and Phase 2's portfolio checkpoint — the same wrap-up discipline as Week 1's Day 7, now applied to a meaningfully more complex pipeline.

---

## DAY 12 — Phase 2 Completion: Cold-Start Validation, Merge, Portfolio Checkpoint

### 1. Daily Goal
Prove the entire ingestion → Bronze → Soda quality gate chain works from a genuinely cold start, merge the feature branch, and bring the README and portfolio artifacts up to date with real, working Delta Lake infrastructure.

### 2. Why Today's Work Matters
Exactly the same reasoning as Week 1's Day 7, now applied to a bigger, more consequential slice of the system: a "works on my machine, in my current terminal session" Bronze layer is not the same thing as a genuinely reproducible one. Today's cold-start test is what actually earns the right to call Phase 2 done.

### 3. Learning Objectives
- Practice the same rigorous cold-start validation discipline from Week 1, now against a multi-step pipeline (ingestion → Bronze → quality gate)
- Perform a real self-review of a larger, more consequential piece of code than Week 1's ingestion script
- Update project documentation to accurately reflect a meaningfully more sophisticated system than a week ago

### 4. Concepts to Understand First
No new external concepts today — this is a consolidation and validation day, exactly like Day 7. The one thing worth being deliberate about: today's cold-start test is more complex than Day 7's, because it now spans two scripts and a quality-gate step with real dependencies between them. Treat "the whole chain works from zero" as the actual unit you're testing, not each script individually — you already unit-tested each piece this week.

### 5. Official Documentation to Read
None new — re-read your own `docs/design_decisions.md` entries from Days 8–11 and reconcile them against what you actually built, exactly as you did on Week 1's Day 7.

### 6. YouTube Topics to Study
None required today.

### 7. Building Tasks

**Task 1 — Full cold-start test: ingestion → Bronze → Soda gate**
- *What:* `docker compose down -v && docker compose up -d`; recreate buckets; run `kaggle_ingest.py`; run `bronze_transform.py`; run the Soda scan against `checks/bronze_checks.yml`
- *Why:* the definitive proof that this week's work is genuinely reproducible, not accidentally dependent on state built up over several days of iterative development
- *Expected output:* all three steps succeed in sequence with no manual intervention beyond running each command
- *Verify:* final Soda scan passes, `DESCRIBE HISTORY` shows a single clean version 0 write (confirming no leftover state from earlier in the week)
- *Common mistake:* discovering a step that implicitly depended on a file or bucket state you created manually earlier in the week and forgot to script — if you find one, fix the root cause today, don't patch around it

**Task 2 — Self-review `bronze_transform.py` as if reviewing a colleague's PR**
- *What:* read the full script top to bottom checking for: dead code, leftover debug prints, inconsistent naming vs. `kaggle_ingest.py`'s established style, any hardcoded values that should be config/env-driven
- *Expected output:* a short cleanup list, then actually fix each item
- *Verify:* the script reads cleanly with no leftover exploration artifacts

**Task 3 — Verify Bronze schema and lineage against the Day 8 design doc**
- *What:* re-check that the implemented schema and the four lineage columns exactly match what you specified in `docs/design_decisions.md`
- *Expected output:* confirmed match, or a documented, deliberate reconciliation note if anything diverged

**Task 4 — Merge and tag**
- *Steps:* merge `feature/bronze-delta-layer` into `main`, delete the branch, `git tag v0.3-phase2-complete && git push --tags`

**Task 5 — Update the README's Progress section**
- *What:* add Phase 2 as complete; expand the "Running Locally" section to include the Bronze and Soda scan commands; add a short "Architecture at this point" note describing Raw → Bronze (Delta) with a quality gate — this is also a good moment to add a simple text-based or hand-drawn architecture diagram showing just what exists so far (Raw → Bronze), rather than waiting to diagram the entire eventual system at once
- *Verify:* someone cloning the repo fresh, with a valid `.env`, could follow the README to reproduce ingestion + Bronze + quality gate

**Task 6 — Portfolio screenshots**
- *What:* screenshot the MinIO console showing the Delta table structure (`_delta_log/` + partitioned data), and a Soda Core scan output showing all checks passing
- *Expected output:* both saved to `docs/`

### 8. Definition of Done
- [ ] Full cold-start test (ingestion → Bronze → Soda gate) succeeds with zero manual fixes
- [ ] Self-review completed, cleanup items resolved
- [ ] Schema/lineage implementation reconciled against Day 8's design doc
- [ ] `feature/bronze-delta-layer` merged into `main`, tagged `v0.3-phase2-complete`
- [ ] README updated: Phase 2 progress, run instructions, early architecture note
- [ ] Portfolio screenshots saved

### 9. Validation Steps
Same honest bar as Week 1's Day 7: hand the repo (conceptually) to another engineer with only a valid `.env` and your README — would they get a working Bronze Delta table with a passing quality gate by following it exactly? If not, the README isn't done yet, regardless of whether the code is.

### 10. Common Beginner Mistakes
- Merging without re-testing cold-start first
- Diagramming the *entire* eventual architecture today instead of just what's actually built so far — an aspirational diagram that doesn't match current reality undermines the honest-progress signal you built carefully in Week 1
- Letting Phase 2's README update be vaguer than Phase 1's was

### 11. Debugging Guide
If cold-start fails specifically at the Soda scan step, re-verify the DuckDB `delta` extension loads correctly in a fresh container/environment, not just your already-configured local shell from earlier in the week — this is exactly the kind of "implicit local state" the cold-start test exists to catch.

### 12. Mentor Notes
- "You've now run this exact cold-start discipline twice — once for a single script (Week 1), once for a three-step chain with a real dependency between steps (today). Notice how much faster and more confident this felt the second time. That's not a coincidence — you're building a genuine engineering habit, not just following instructions."
- "The decision to diagram only what's actually built so far, rather than the whole eventual system, is a small thing that signals real engineering maturity to anyone reviewing your repo closely. Aspirational documentation that outpaces working code is one of the most common ways portfolio projects lose credibility on close inspection."

### 13. Industry Insights
- Teams that maintain "living" architecture diagrams (updated alongside the code, not written once at the start) are the exception rather than the rule in industry — doing this consistently, even in a solo portfolio project, is a genuinely above-average practice worth naming explicitly if asked about your documentation habits in an interview.

### 14. Git Workflow
- **Branch:** `main` (post-merge)
- **Commit/merge:** `feature/bronze-delta-layer` → `main`
- **Tag:** `v0.3-phase2-complete`

### 15. README Updates
Completed as Task 5 above — Phase 2 progress, expanded run instructions, early architecture note.

### 16. Portfolio Updates
Two new screenshots saved (`docs/day12_...`), README reflects real, current, working state through Bronze.

### 17. Interview Questions
- "Walk me through what happens, end to end, from raw data landing in your system to it being available in a validated Bronze table."
- "How do you personally validate that a multi-step pipeline is genuinely reproducible, not just 'working right now on your machine'?"

### 18. Learning Checkpoint
- What's different about testing a three-step chain's reproducibility versus a single script's, and why did today's cold-start test need to check for state built up gradually over several days?
- Why does an honestly-scoped, currently-accurate architecture diagram matter more than a complete, aspirational one at this stage?

### 19. End-of-Day Checklist
- [ ] All 6 tasks complete
- [ ] Cold-start chain proven end to end
- [ ] Merged, tagged, README and portfolio updated

### 20. Tomorrow's Preview
Day 13 shifts from building to designing again — exactly like Day 4's ingestion design day — this time specifying precisely how Silver's incremental MERGE, watermarking, and content-hash deduplication will work, before any of that code gets written on Day 14.

---

## DAY 13 — Silver Design Day: Incremental Processing, Watermarks, and Deduplication Strategy

### 1. Daily Goal
Design, in writing, exactly how Silver's incremental MERGE-based processing will work — the watermark mechanism, the deduplication key, and the business rule filters — before writing any implementation code.

### 2. Why Today's Work Matters
Silver is the most conceptually demanding part of this entire project. The Implementation Guide's Section 0 review named "full overwrite processing" as one of v1.0's core production gaps, and incremental MERGE logic is the fix — but it's also genuinely easy to get subtly wrong (a watermark updated at the wrong point, a dedup key that isn't actually unique). Exactly like Day 4's ingestion design day prevented ambiguity during implementation, today prevents you from discovering fundamental design flaws mid-code tomorrow, when they're much more expensive to unwind.

### 3. Learning Objectives
- Understand watermarking precisely enough to explain why the watermark must only advance *after* a successful MERGE, not before
- Design a deduplication key for data that has no natural primary key
- Understand Delta's MERGE INTO semantics well enough to specify exact match/update/insert behavior before writing SQL

### 4. Concepts to Understand First

**Watermarking, precisely** — a watermark is simply "the furthest point in the source data we've successfully processed so far," stored durably (a small Delta table or a marker file) so a re-run of the job knows where to resume. The critical, easy-to-get-wrong detail: the watermark must be read *before* processing begins, and only written/advanced *after* the MERGE that used it has fully succeeded. If you advance it first, "just to be safe," a mid-job crash leaves you having marked data as processed that never actually was — a subtle, hard-to-detect form of silent data loss. This is the single most important design decision to get right today, in writing, before any code exists tomorrow.

**Why clickstream data has no natural primary key, and what a synthetic key needs to guarantee** — unlike `trip_id` in taxi data, no single column in this dataset uniquely identifies an event. The Implementation Guide specifies a synthetic key: a hash of `(user_id, event_type, product_id, event_time)`. For this to work as a MERGE key, it needs to be **deterministic** (the same logical event always produces the same hash, even across different pipeline runs) and have a **low realistic collision rate** given this dataset's actual granularity. Today, you reason through both properties in writing, rather than assuming a hash "just works" as a key.

**MERGE INTO semantics** — a Delta `MERGE INTO target USING source ON <match condition> WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` statement needs a precise match condition (your synthetic key, comparing source and target), and precise behavior for both branches. For this project: if a row's synthetic key already exists in Silver, what should happen — should the row ever be *updated* (unlikely, since clickstream events are immutable once they happen, unlike e.g. an order status that can change), or should MATCHED simply mean "already present, do nothing new"? Deciding this precisely today avoids writing ambiguous MERGE logic tomorrow.

### 5. Official Documentation to Read
1. Delta Lake docs — "Upsert into a table using merge," read the full page carefully today, since tomorrow is implementation
2. Delta Lake docs — "Table streaming reads and writes" section on watermarking (even though you're using batch, not streaming, the watermarking *concept* is described clearly there and transfers directly)

### 6. YouTube Topics to Study
- "Delta Lake MERGE INTO upsert tutorial"
- "Data pipeline watermarking incremental processing explained"
- "Deterministic hashing for synthetic surrogate keys"

### 7. Building Tasks

**Task 1 — Design the watermark storage mechanism**
- *What:* decide and document: where does the watermark live (a small Delta table `silver/_watermarks/` with columns `pipeline_name`, `last_processed_event_time`, `updated_at`, seems appropriate — but decide deliberately, don't just accept this suggestion uncritically), and exactly what value it tracks (max `event_time` successfully processed, not max `ingested_at` — reasoning through *why* matters: `event_time` reflects the actual data content processed, which is what you need to correctly bound the next read from Bronze)
- *Why:* a watermark stored as a proper Delta table (not a flat file) gets the same ACID write guarantees as your data tables — a crash mid-update to the watermark table can't leave it in a half-written state
- *Expected output:* a written spec in `docs/design_decisions.md` describing the watermark table's schema and exact update semantics
- *Verify:* re-read it and ask: "does this spec make it unambiguous exactly when the watermark advances, relative to the MERGE?" If there's any ambiguity, resolve it in writing now

**Task 2 — Design the synthetic deduplication key**
- *What:* specify, precisely, the exact hash function (SHA-256) and exact field concatenation order and format for `(user_id, event_type, product_id, event_time)` — precision matters here: are timestamps hashed as strings in a specific format, or as epoch integers? An inconsistency here between two different runs would silently break determinism
- *Why:* this level of precision is what makes the key actually deterministic across separate pipeline runs, which is the property the whole MERGE strategy depends on
- *Expected output:* a written spec with the exact hashing approach, precise enough that two different implementations (yours today, and a hypothetical teammate's) would produce identical hash values for the same input row
- *Verify:* by hand, compute the hash for one real sample row using your specified method, twice, independently — confirm you get the same result both times (this previews tomorrow's actual test, done manually today to validate the *design* before the *code*)

**Task 3 — Specify the MERGE INTO match/update/insert behavior precisely**
- *What:* write out, in plain English first and then as near-pseudocode SQL, the exact MERGE statement's behavior: `ON target.event_key = source.event_key WHEN NOT MATCHED THEN INSERT *` (and explicitly decide: no `WHEN MATCHED` update clause at all, since clickstream events are immutable — document *why* you're deliberately omitting an update branch, since an interviewer might reasonably ask "what happens on a match" and "nothing, by design, because these events never change after the fact" is a real, considered answer, not an oversight)
- *Expected output:* a written MERGE specification in `docs/design_decisions.md`
- *Verify:* re-read it against Task 2's key design — does the match condition use exactly the synthetic key you specified?

**Task 4 — Specify the business rule filters precisely**
- *What:* write out the exact filter conditions the Implementation Guide specifies (null `user_session` dropped, negative `price` dropped, `event_time` outside a valid range dropped) as precise, testable predicates, and specify *where* in the pipeline they apply — before or after deduplication? (Reasoning it through: filtering first, then deduplicating the remainder, is more efficient and avoids wasted hash computation on rows you're going to discard anyway — decide this order deliberately and document why)
- *Expected output:* an ordered list of filter steps with exact predicates, in `docs/design_decisions.md`

**Task 5 — Specify category_code parsing**
- *What:* the raw `category_code` field is dot-delimited (e.g., `electronics.smartphone`); specify exactly how you'll parse it into `category_l1`/`category_l2`/`category_l3`, and explicitly decide what happens when a row has fewer than 3 levels (a very real case in this dataset) — nulls for missing levels, decided and documented now, not discovered as a surprise tomorrow
- *Expected output:* parsing spec added to `docs/design_decisions.md`

**Task 6 — Reconcile the full Silver design against Section 8 of the Implementation Guide**
- *What:* re-read the "Staging" and Silver-related sections of the Implementation Guide, and confirm today's design decisions are consistent with it (e.g., that enriched `category_l1/l2/l3` columns are indeed expected downstream)
- *Verify:* no contradictions between your detailed design and the guide's higher-level architecture

**Task 7 — Commit**
- *Steps:* `git checkout -b feature/silver-incremental-layer && git add docs/design_decisions.md && git commit -m "docs: design Silver incremental MERGE, watermarking, deduplication key, and business rule filters"`

### 8. Definition of Done
- [ ] Watermark storage mechanism and exact update timing specified in writing
- [ ] Synthetic deduplication key precisely specified, with a manual hash computation verified reproducible by hand
- [ ] MERGE INTO match/update/insert behavior specified precisely, including the deliberate decision to omit an update branch
- [ ] Business rule filters specified as exact, ordered predicates
- [ ] category_code parsing behavior for edge cases (fewer than 3 levels) decided and documented
- [ ] Full design reconciled against Implementation Guide Section 8
- [ ] New feature branch created and committed

### 9. Validation Steps
Hand your written Silver design spec a genuine "could someone else implement this from my notes alone" test, the same bar as Day 4's ingestion spec — if any part requires you to mentally fill in a gap while reading it back, that gap needs to be resolved in writing before Day 14.

### 10. Common Beginner Mistakes
- Treating "hash the fields together" as sufficiently precise without specifying exact format/order — this is where silent non-determinism bugs come from
- Not deciding, explicitly, whether the watermark tracks `event_time` or `ingested_at` (these are meaningfully different and the wrong choice would incorrectly bound future reads)
- Skipping the manual hash-by-hand verification step and only discovering a determinism issue once real code is running tomorrow

### 11. Debugging Guide
No implementation exists yet today, so there's nothing to debug in the traditional sense — the "debugging" today is entirely about stress-testing your own design in writing: read each spec section and actively try to find an ambiguity or edge case it doesn't address, the same adversarial mindset as a code self-review, applied to a design doc instead.

### 12. Mentor Notes
- "Today might feel like 'less real work' than the last five days, since nothing runs yet. It's not. The watermark-timing decision alone — advance only after success, never before — is the single most consequential design decision in this entire phase, and it's far cheaper to get right in a paragraph today than to discover wrong after a mid-job crash silently drops data three weeks from now."
- "The deliberate decision to omit a `WHEN MATCHED` update branch, and to be able to explain *why*, is exactly the kind of design reasoning that separates 'I copied a MERGE example from documentation' from 'I understood my data well enough to know which parts of the general pattern apply to my specific case.' Interviewers can tell the difference immediately when they ask a follow-up question."

### 13. Industry Insights
- Watermark-based incremental processing, done correctly, is one of the most commonly probed real-world DE competencies in interviews precisely because it's easy to describe correctly in theory and easy to get subtly wrong in practice — being able to say "the watermark only advances after a successful commit, here's why" from direct design experience carries real weight.
- Immutable-event design (deciding "these facts never update once written, only new facts get appended") is a foundational idea behind event-sourcing architectures more broadly — recognizing this pattern by name, and that today's `WHEN NOT MATCHED THEN INSERT`-only design is an instance of it, is a good connection to be able to draw in a system-design conversation.

### 14. Git Workflow
- **Branch:** `feature/silver-incremental-layer` (new)
- **Commit message:** `docs: design Silver incremental MERGE, watermarking, deduplication key, and business rule filters`
- **Merge:** not yet
- **Tag:** none today

### 15. README Updates
None today — design work, not a shippable increment.

### 16. Portfolio Updates
None today.

### 17. Interview Questions
- "How would you design a watermark for incremental batch processing, and what's the most important thing to get right about when it updates?"
- "How do you design a synthetic key for data with no natural primary key, and what properties does it need?"
- "Why might you deliberately choose not to handle the 'matched' case in a MERGE statement?"

### 18. Learning Checkpoint
- Explain, precisely, why the watermark must only advance after a successful MERGE, using a concrete example of what goes wrong if it advances first.
- What exact fields, in what exact format, make up your synthetic deduplication key?
- Why did you decide clickstream events don't need an update branch in the MERGE statement?

### 19. End-of-Day Checklist
- [ ] All 7 tasks complete
- [ ] Manual hash-by-hand determinism check performed
- [ ] Full design self-reviewed for ambiguity
- [ ] Committed on new feature branch

### 20. Tomorrow's Preview
Day 14 turns today's design into the first real code: reading the watermark, filtering Bronze rows newer than it, applying today's business rule filters and category parsing, and computing the synthetic deduplication key — everything up to (but not including) the actual MERGE write, which is the first task of Week 3.

---

## DAY 14 — Silver Transform Part 1: Watermark Read, Filtering, and Deduplication Key Computation

### 1. Daily Goal
Implement the first half of `spark_jobs/silver_transform.py`: reading the watermark, pulling only newer Bronze rows, applying business rule filters and category parsing, and computing the synthetic deduplication key — validated against real data, stopping short of the actual MERGE write.

### 2. Why Today's Work Matters
This is where yesterday's precise design becomes real, testable code — and where you'll discover, hands-on, whether your Day 13 design genuinely holds up against real data or needs a small correction. Stopping deliberately before the MERGE write today (rather than trying to do it all in one day) means you can fully validate the trickiest, most novel part of the logic — the deduplication key and business filters — in isolation, exactly the same "isolate what you're least confident about" principle from Week 1's Day 5.

### 3. Learning Objectives
- Implement watermark-bounded reads from a Delta table
- Implement the exact business rule filters and category parsing designed yesterday
- Implement and verify deterministic hash-based deduplication key computation against real data at scale (not just the one hand-computed row from yesterday)

### 4. Concepts to Understand First

**Reading "only new" rows from Bronze** — with the watermark table from yesterday's design, today's read becomes: read the current watermark value, then `bronze_df.filter(col("event_time") > watermark_value)`. The first time this runs, with no watermark yet stored, it should process everything (treat a missing watermark as "beginning of time") — decide and implement this initial-run behavior explicitly, since it's an edge case Day 13's design should have addressed (if it didn't explicitly, that's fine, resolve it now and note it as a deliberate addition).

**`sha2()` in Spark for deterministic hashing** — Spark's built-in `sha2(concat_ws("|", col1, col2, ...), 256)` function is the direct implementation of yesterday's design; the `concat_ws` delimiter choice matters (must be a character that can't appear inside any of the concatenated fields, to avoid two different field combinations accidentally producing the same concatenated string before hashing) — this is a subtle correctness detail worth being deliberate about, not just picking `"|"` because it looks reasonable.

**Validating determinism at scale, not just by hand** — yesterday you proved your hash design was deterministic for one row, by hand. Today's real test: compute the hash for the *same* real dataset twice, in two separate Spark job runs, and confirm every row gets an identical hash both times. This is the actual proof that matters, since a bug that only shows up at scale (e.g., a locale-dependent string formatting difference) wouldn't necessarily show up in a single hand-computed example.

### 5. Official Documentation to Read
1. Spark SQL functions reference — `sha2`, `concat_ws`
2. Delta Lake docs — reading a Delta table with a filter condition (mostly review of what you already know from Bronze, applied to a read instead of a write)

### 6. YouTube Topics to Study
- "PySpark sha2 concat_ws hash column"
- "PySpark window functions partitioning" (useful preview for Week 3's sessionization work in dbt, but the filtering concepts here are related enough to warm up on now)

### 7. Building Tasks

**Task 1 — Create `spark_jobs/silver_transform.py` with the same CLI/logging pattern as Bronze**
- *What:* argparse skeleton, structured logging, matching the established house style from Days 5 and 9
- *Expected output:* runs with `--help`, logs a structured "job started" event
- *Verify:* consistent logging shape with your other two scripts

**Task 2 — Implement watermark read with the missing-watermark edge case handled**
- *What:* read the current watermark value from the `silver/_watermarks/` Delta table (from yesterday's design); if no row exists yet, treat the effective watermark as a very early date (or explicitly handle it as "no filter, read everything")
- *Why:* exactly as discussed above — this is a first-run edge case your design should account for explicitly
- *Expected output:* a function returning the correct watermark value, or a documented sentinel for "no watermark yet"
- *Verify:* run it against your currently-empty watermark table (since you haven't written to it yet) and confirm it correctly falls back to "process everything," not an error

**Task 3 — Read Bronze rows newer than the watermark**
- *What:* filter Bronze on `event_time > watermark_value`
- *Expected output:* on this first run, since the watermark is effectively "empty," this should return your full Bronze dataset
- *Verify:* row count matches your Bronze table's total valid row count from Day 12

**Task 4 — Implement business rule filters, in the order specified yesterday**
- *What:* apply the null-`user_session`, negative-`price`, and `event_time`-range filters, logging a count of rows dropped by each specific rule (not just a combined total — you want to know *which* rule is doing the most filtering, both for debugging today and for genuine operational visibility later)
- *Expected output:* a filtered DataFrame, plus structured log lines showing rows dropped per rule
- *Verify:* the numbers make sense given what you know about the real dataset — if one rule is unexpectedly dropping a huge fraction of rows, investigate before proceeding, don't just accept it

**Task 5 — Implement category_code parsing**
- *What:* split `category_code` on `.` into `category_l1`/`l2`/`l3`, handling the fewer-than-3-levels case exactly as specified yesterday
- *Expected output:* three new columns, correctly null where a level doesn't exist
- *Verify:* `.show()` several real rows with varying numbers of category levels and confirm correct parsing in each case, including the edge case

**Task 6 — Compute the synthetic deduplication key**
- *What:* implement `sha2(concat_ws(<your chosen delimiter>, user_id, event_type, product_id, event_time), 256)` exactly per yesterday's spec
- *Expected output:* a new `event_key` column
- *Verify (the important one):* run the job twice against the same input data, save both outputs, and confirm every row's `event_key` value is byte-identical between the two runs — this is today's real proof of determinism at scale, not just Day 13's single hand-computed row

**Task 7 — Check for any duplicate `event_key` values within a single run**
- *What:* `df.groupBy("event_key").count().filter(col("count") > 1)`
- *Why:* if genuinely duplicate events exist in the real source data (the same logical event appearing twice, e.g., from an upstream retry before it ever reached Kaggle's dataset), you should see them here as key collisions — and this is actually the *correct*, expected behavior of your key design, not a bug: two truly identical events should produce the same key
- *Expected output:* a report of how many duplicate keys exist in this real dataset, if any
- *Verify:* manually inspect a few duplicate groups (if any exist) and confirm they really are the same logical event, not a false-positive hash collision between genuinely different events (extremely unlikely with SHA-256 at this data volume, but worth a sanity glance)

**Task 8 — Commit (without a MERGE write yet — that's Week 3)**
- *Steps:* `git add spark_jobs/silver_transform.py && git commit -m "feat: implement Silver watermark read, business rule filters, category parsing, and deterministic dedup key computation"`

### 8. Definition of Done
- [ ] Watermark read implemented with the missing-watermark edge case correctly handled
- [ ] Business rule filters implemented with per-rule drop counts logged
- [ ] Category parsing implemented and verified against real edge cases (fewer than 3 levels)
- [ ] Synthetic deduplication key implemented and proven deterministic across two separate runs on real data
- [ ] Duplicate key report generated and manually spot-checked
- [ ] Commit made

### 9. Validation Steps
Beyond Task 6's two-run determinism check: restart the Spark container entirely between the two runs (not just re-run in the same session) to rule out any in-session caching artificially making results look consistent when they wouldn't be across genuinely separate executions.

### 10. Common Beginner Mistakes
- Choosing a `concat_ws` delimiter that could plausibly appear inside one of the concatenated fields
- Not testing determinism across genuinely separate runs/sessions, only within one
- Treating found duplicate keys as automatically a bug, without checking whether they represent real, expected duplicate events

### 11. Debugging Guide
If the two determinism-check runs produce different hashes for the same logical rows, suspect a non-deterministic upstream step first — check whether `event_time`'s string representation could vary between runs (e.g., a timezone-dependent formatting difference) before suspecting Spark's `sha2` function itself, which is deterministic by design. If category parsing produces unexpected nulls for a level that should exist, print the raw `category_code` value for a few examples — you may have missed a formatting variant during yesterday's design.

### 12. Mentor Notes
- "The two-run determinism test in Task 6 is today's version of Day 5's 'test the failure path, not just the happy path' principle, applied to a positive property (determinism) rather than a failure mode. Proving a property holds across genuinely separate executions, not just within one convenient session, is a habit that will save you from a whole category of 'works in dev, breaks in prod' bugs throughout your career."
- "Finding real duplicate keys in Task 7 and confirming they represent genuine duplicate events, rather than panicking that your design is broken, is exactly the kind of calm, evidence-based debugging instinct a senior engineer has. The data telling you something surprising isn't automatically a bug in your code — sometimes it's your code correctly revealing something true and previously invisible about the data."

### 13. Industry Insights
- Determinism bugs caused by locale- or timezone-dependent string formatting are a genuinely common, hard-to-catch class of production incident — teams that have been burned by one usually add an explicit "always format timestamps as UTC ISO-8601 before hashing or comparing" rule to their engineering standards, which is effectively what you're practicing today.
- Being able to say, in an interview, "I found real duplicate events in my source data and verified they were genuine rather than assuming a bug" demonstrates the kind of skeptical-but-not-paranoid data instinct that's hard to fake with a purely tutorial-following project.

### 14. Git Workflow
- **Branch:** `feature/silver-incremental-layer`
- **Commit message:** `feat: implement Silver watermark read, business rule filters, category parsing, and deterministic dedup key computation`
- **Merge:** not yet — this branch stays open into Week 3, where the MERGE write, watermark advancement, and Silver-layer Soda checks complete Phase 3
- **Tag:** none today

### 15. README Updates
None yet — Silver isn't functionally complete until the MERGE write exists (Week 3).

### 16. Portfolio Updates
None yet — Week 3 will produce Silver's real portfolio artifacts once the layer is fully functional.

### 17. Interview Questions
- "How would you prove, rigorously, that a hash-based key generation step is genuinely deterministic across separate pipeline runs, not just within one session?"
- "Tell me about a time real data revealed something you didn't expect. How did you distinguish 'the data is telling me something true' from 'I have a bug'?"

### 18. Learning Checkpoint
- Why does the first-ever run need explicit handling for a missing watermark, and what did you decide that behavior should be?
- Walk through, from memory, the exact fields and delimiter used in your deduplication key, and explain why the delimiter choice matters.
- What did finding (or not finding) duplicate keys in Task 7 tell you about the real dataset?

### 19. End-of-Day Checklist
- [ ] All 8 tasks complete
- [ ] Determinism proven across two genuinely separate runs
- [ ] Duplicate key findings understood and explained, not just observed
- [ ] Committed on `feature/silver-incremental-layer`

### 20. Tomorrow's Preview
Week 3 opens by finishing Silver: implementing the actual `MERGE INTO` write using the key and filtered/enriched data built this week, advancing the watermark only after a successful MERGE (exactly as designed on Day 13), wiring in Silver-layer Soda Core checks, and closing out Phase 3 with the same cold-start validation discipline you've now applied twice.

---

## 📦 WEEK 2 REVIEW

### Engineering Milestone Achieved
You now have a real, working Delta Lake Bronze layer — schema-enforced, lineage-tracked, quarantine-aware, and gated by an automated Soda Core quality check with a proven, working failure mode. You've also completed the hardest design and validation work for Silver's incremental MERGE logic: a precisely specified, proven-deterministic deduplication key, and correctly filtered, enriched data ready for the MERGE write that opens Week 3.

### Skills Gained This Week
- Explicit schema design and enforcement, and the reasoning behind quarantine-over-silent-drop
- Delta Lake's transaction log, ACID writes, versioning, and time travel — understood conceptually *and* proven hands-on
- Correct partitioning strategy reasoning (date-based vs. high-cardinality)
- Lineage metadata design, and the event-time vs. processing-time distinction
- Soda Core check authoring across schema, null, freshness, and volume categories, with genuine failure-path verification
- Watermark-based incremental processing design, including the critical "advance only after success" timing decision
- Deterministic synthetic key design and hands-on, at-scale proof of determinism
- A second full round of the cold-start validation discipline, now applied to a multi-step pipeline with real inter-step dependencies

### Portfolio Progress
`main` now contains a genuinely working, cold-start-verified Bronze layer with an automated quality gate — three tagged milestones exist (`v0.1`, `v0.2`, `v0.3`), each representing a real, working increment, not just a checkpoint for its own sake. The README accurately reflects a Raw → Bronze (Delta) architecture with quality gating, and portfolio screenshots exist showing real Delta table structure and passing quality checks. The `feature/silver-incremental-layer` branch holds substantial, well-tested design and partial implementation work, intentionally not yet merged since Silver isn't functionally complete.

### Readiness Checklist for Week 3
Before Week 3 begins, confirm:
1. A full cold-start test (ingestion → Bronze → Soda gate) still passes cleanly.
2. You can explain, without notes, why the watermark must only advance after a successful MERGE.
3. You can explain, without notes, what makes your synthetic deduplication key deterministic, and you've proven it across genuinely separate runs.
4. `main` is merged through Phase 2 and tagged `v0.3-phase2-complete`; `feature/silver-incremental-layer` exists with Day 13–14's work committed.
5. You're comfortable with `DESCRIBE HISTORY`, time travel queries, and reading Soda Core scan output — these all get used again, without re-explanation, starting Week 3.

**Confirm these are genuinely true and I'll generate Week 3 — completing the Silver MERGE write, Silver-layer quality gates, and the start of dbt-based data modeling.**
-e 

---


### Chapter 3 — Week 3: Completing Silver & Formalizing the Data Quality Framework

---

# WEEK 3 — Silver MERGE, Crash-Safety, and an End-to-End Quality Gate

**Week 3 maps to:** Implementation Guide Phase 3 completion (Silver Incremental MERGE, Days 15–18) + Phase 4 (Data Quality Framework, End-to-End, Days 19–21)
**Where we left off:** Bronze is merged, tagged `v0.3-phase2-complete`, and cold-start verified with a working Soda gate. On `feature/silver-incremental-layer`, you have a precisely specified, proven-deterministic watermark design and deduplication key, plus working code for the watermark read, business rule filters, category parsing, and dedup key computation — everything short of the actual write.
**By the end of this week you will have:** a genuinely incremental Silver Delta table, written via MERGE, with a watermark that advances only on verified success and survives a simulated mid-job crash correctly — plus a complete, reusable, three-layer Soda Core quality framework (Raw, Bronze, Silver) with its own aggregating gate script, ready to be wired into Airflow in the weeks ahead.

---

## DAY 15 — Silver Transform Part 2: The MERGE Write and Watermark Advancement

### 1. Daily Goal
Implement the actual `MERGE INTO` write from Day 13's design, and implement watermark advancement that happens strictly after — and only after — that MERGE succeeds.

### 2. Why Today's Work Matters
This is the moment the single most important design decision of Week 2 (Day 13: "the watermark must only advance after a successful MERGE") becomes real, ordered code. Get the sequencing right today, under no time pressure, and you'll never have to reason about it again for the rest of the project — Airflow will just call this script, and the script's own internal correctness is what protects you.

### 3. Learning Objectives
- Implement a Delta `MERGE INTO` statement matching Day 13's exact specification
- Implement strict operation ordering: MERGE completes and is confirmed successful *before* the watermark table is touched
- Understand what "confirmed successful" concretely means in PySpark — not just "no exception was raised," but a genuine post-write check

### 4. Concepts to Understand First

**PySpark's `DeltaTable` Python API for MERGE** — you'll use `DeltaTable.forPath(spark, path)` to get a handle to the existing Silver Delta table, then call `.merge(source_df, "target.event_key = source.event_key").whenNotMatchedInsertAll().execute()` — this is the Python-native equivalent of the SQL `MERGE INTO` statement you specified in pseudocode on Day 13. Notice this directly implements yesterday's decision to omit a `WHEN MATCHED` clause entirely: `.whenNotMatchedInsertAll()` with no matching `.whenMatchedUpdate(...)` call is the code-level expression of "matched rows are left alone, by design."

**What "confirmed successful" means beyond "no exception raised"** — `.execute()` completing without throwing is necessary but not sufficient proof of success for a decision as consequential as advancing the watermark. Today you'll also verify the operation actually happened as expected — for example, by checking `DESCRIBE HISTORY`'s latest entry shows the operation type `MERGE` with a plausible `operationMetrics` row count, immediately after the `.execute()` call, before touching the watermark. This closes the loop between "the call returned" and "the write is genuinely durable and matches what I expect," the same skepticism you applied to Soda's exit codes on Day 11 and to hash determinism on Day 14.

**Watermark advancement as its own explicit, separate operation** — write the new watermark value (the max `event_time` from the source batch just merged) as a small, separate Delta write to `silver/_watermarks/`, using `overwrite` mode for that single pipeline's row (or an upsert if you're tracking multiple pipelines in one table, matching Day 13's schema design) — and place this call, in your code, unambiguously *after* the post-MERGE verification step from above, not interleaved with it.

### 5. Official Documentation to Read
1. Delta Lake docs — "Update Delta Lake table schema" is not needed today, but re-read "Upsert into a table using merge," this time focused specifically on the Python `DeltaTable` API examples (you read the conceptual/SQL version on Day 13; today's implementation uses the Python API)
2. Delta Lake docs — `DESCRIBE HISTORY` operationMetrics fields reference (to know exactly which metric key holds row counts for a MERGE operation specifically, which differs slightly from a WRITE operation's metrics)

### 6. YouTube Topics to Study
- "Delta Lake Python API merge whenNotMatchedInsertAll"
- "Delta Lake operationMetrics MERGE vs WRITE"

### 7. Building Tasks

**Task 1 — Create the Silver Delta table for the first time, if it doesn't already exist**
- *What:* add a branch in your script: if `silver/ecommerce_events` doesn't exist yet as a Delta table, perform an initial `write` (not merge — you can't MERGE into a table that doesn't exist yet) using the fully filtered/enriched/keyed DataFrame from Day 14's work; if it does exist, skip straight to the MERGE path
- *Why:* every MERGE-based pipeline needs a bootstrap case for its very first run — deciding and implementing this explicitly, rather than assuming the table always already exists, is exactly the kind of edge case Day 13's design work should make you alert to
- *Expected output:* on a fresh environment, the first run creates the Silver Delta table via a plain write; subsequent runs use MERGE
- *Verify:* test both paths — delete the Silver table entirely and run once (should create it), then run again (should MERGE against the now-existing table)
- *Common mistake:* trying to MERGE unconditionally and getting a confusing "table not found" error on the very first run — handle this explicitly rather than treating it as an exceptional error case

**Task 2 — Implement the MERGE INTO write, exactly per Day 13's spec**
- *What:* using `DeltaTable.forPath` and `.merge(...).whenNotMatchedInsertAll().execute()`, matching on your `event_key` column exactly as specified
- *Expected output:* new, previously-unseen rows are inserted; rows whose `event_key` already exists in Silver are left untouched
- *Verify:* run against a real batch, then check that Silver's row count increased by exactly the number of genuinely new rows (accounting for the small number of real duplicate keys you found on Day 14 — those should correctly *not* be re-inserted)
- *Common mistake:* accidentally using `.whenMatchedUpdateAll()` "just to be safe" — resist this; you deliberately decided against it on Day 13, and adding it now would silently contradict yesterday's reasoned design

**Task 3 — Verify the MERGE via `DESCRIBE HISTORY` before touching the watermark**
- *What:* immediately after `.execute()`, query `DESCRIBE HISTORY` for the latest entry, confirm `operation = 'MERGE'` and that `operationMetrics` shows a plausible `numTargetRowsInserted` count
- *Why:* this is the "confirmed successful, not just no exception" check described above
- *Expected output:* a verification step that would catch a scenario where `.execute()` "succeeds" technically but something is clearly wrong with the result (e.g., zero rows inserted when you expected many)
- *Verify:* deliberately compare this count against your own independent count of "how many new rows did I expect," computed from the source DataFrame before the merge — do these numbers agree?

**Task 4 — Implement watermark advancement, strictly after Task 3's verification**
- *What:* compute `max(event_time)` from the source batch just merged, and write it to the watermark table — placed, in your code's actual execution order, only after Task 3's check has passed
- *Expected output:* the watermark table now reflects this run's furthest processed point
- *Verify:* query the watermark table directly and confirm the value matches the max `event_time` of the batch you just processed

**Task 5 — Add explicit, structured logging around this exact sequence**
- *What:* log distinct structured events for: "merge started," "merge completed, verifying," "merge verified, N rows inserted," "watermark advanced to X" — four distinct, ordered log lines that make the sequencing from Tasks 2–4 visible and auditable just by reading the logs
- *Why:* this log sequence is what would let you, or a future teammate, diagnose exactly how far a failed run got, without needing to read the code — "merge completed, verifying" appearing in the logs but "watermark advanced" never appearing tells you precisely what happened
- *Expected output:* four clear, ordered log lines on a successful run
- *Verify:* read through a successful run's full log output as if you'd never seen the code — does the story it tells match what actually happened?

**Task 6 — Run twice against the same source range and confirm true idempotency**
- *What:* run the full Silver job twice in immediate succession against the same Bronze data (don't add any new Bronze data between runs)
- *Why:* this is the definitive proof that your watermark correctly prevents reprocessing — the second run should see a watermark that already covers this range and process either zero new rows, or correctly recognize (via Task 3's dedup) that nothing genuinely new exists
- *Expected output:* Silver's row count is identical after both runs
- *Verify:* explicitly diff row counts before/after the second run — should be exactly zero difference

**Task 7 — Commit**
- *Steps:* `git add spark_jobs/silver_transform.py && git commit -m "feat: implement Silver MERGE write with post-merge verification and watermark advancement strictly after success"`

### 8. Definition of Done
- [ ] First-run table creation and subsequent-run MERGE paths both implemented and tested
- [ ] MERGE matches Day 13's spec exactly (no `WHEN MATCHED` update branch)
- [ ] Post-merge verification via `DESCRIBE HISTORY` implemented, checked before watermark write
- [ ] Watermark advancement correctly sequenced strictly after verification
- [ ] Four-part structured log sequence implemented and manually reviewed for narrative clarity
- [ ] Two consecutive runs against the same data proven truly idempotent (zero row-count change on the second run)
- [ ] Commit made

### 9. Validation Steps
Run the full sequence a third time, but this time add a small amount of genuinely new synthetic Bronze data (a few hand-crafted rows with new `event_key` values) between runs two and three — confirm the third run's Silver row count increases by exactly the number of new rows added, no more and no less, proving the watermark-plus-MERGE combination handles the realistic "some new data has arrived" case correctly, not just the fully-static-data case from Task 6.

### 10. Common Beginner Mistakes
- Adding a `WHEN MATCHED` update clause "just in case," contradicting the Day 13 design decision
- Treating `.execute()` completing without an exception as sufficient proof of success
- Writing the watermark advancement code in a way where, if you reordered lines later without thinking, it could accidentally run before the merge verification — keep the ordering visually obvious in the code (clear sequential blocks, not intermixed)

### 11. Debugging Guide
If the second run in Task 6 unexpectedly changes the row count, the most likely cause is the watermark not actually being read/applied correctly on that second run (double check you're not accidentally using a stale in-memory watermark value from earlier in the script rather than re-reading it) — or a subtle issue in the `event_key` matching condition not correctly identifying already-present rows. If `DESCRIBE HISTORY`'s `operationMetrics` don't include the field you expect, print the entire metrics map for one real run and inspect it directly, since exact field names can vary slightly by Delta version.

### 12. Mentor Notes
- "Today's four-part logging sequence in Task 5 is a small thing that pays off disproportionately. Months from now, if a run fails at 3am and you're reading logs on your phone half-asleep, 'merge completed, verifying' being the last line you see tells you immediately and unambiguously where things stopped — no code-reading required. This is the actual, practical payoff of the observability thinking from the Implementation Guide's governance section, made concrete."
- "Resisting the urge to add a `WHEN MATCHED` clause 'just in case' today is a good instinct to generalize: once you've made a deliberate, reasoned design decision, changing it later out of vague unease rather than a specific new piece of evidence is usually a mistake. If you ever do want to revisit the decision, revisit it explicitly — write down the new reasoning — rather than quietly hedging."

### 13. Industry Insights
- The pattern of "perform the risky operation, verify its actual result, *then* commit the durable side-effect that depends on it having succeeded" generalizes far beyond this pipeline — it's the same shape as two-phase commit protocols in distributed databases, and the same reasoning behind why payment systems verify a charge succeeded before marking an order as paid, rather than the reverse.
- Being able to describe, precisely, how your pipeline behaves if it crashes *between* the MERGE and the watermark write (answer: the next run reprocesses that range, the MERGE's `event_key` matching makes this safe since already-inserted rows are correctly skipped) is exactly the kind of "what happens when it fails halfway through" question a staff engineer asks in a system design interview.

### 14. Git Workflow
- **Branch:** `feature/silver-incremental-layer`
- **Commit message:** `feat: implement Silver MERGE write with post-merge verification and watermark advancement strictly after success`
- **Merge:** not yet
- **Tag:** none today

### 15. README Updates
None yet — Day 18 covers the full Phase 3 README update once Silver's quality gate also exists.

### 16. Portfolio Updates
None yet.

### 17. Interview Questions
- "Walk me through exactly what your pipeline does if it crashes between writing data and updating its own internal state about what's been processed."
- "Why is 'no exception was thrown' not sufficient proof that a write operation succeeded the way you expected?"

### 18. Learning Checkpoint
- Describe, in exact order, the sequence of operations from MERGE execution through watermark advancement, and explain why that order matters.
- What would happen, concretely, if your job crashed immediately after Task 3's verification but before Task 4's watermark write? Is that safe? Why?
- Why did you deliberately not add a `WHEN MATCHED` clause to the MERGE?

### 19. End-of-Day Checklist
- [ ] All 7 tasks complete
- [ ] True idempotency proven across two identical runs
- [ ] New-data scenario validated (Task 9's extra check)
- [ ] Committed

### 20. Tomorrow's Preview
Day 16 stress-tests today's crash-safety claim directly — simulating a mid-job crash between the MERGE and the watermark advancement, and proving your pipeline recovers correctly on the next run, rather than just reasoning about it in the abstract as today's checkpoint questions asked you to.

---

## DAY 16 — Crash-Safety Validation: Simulating Mid-Job Failure

### 1. Daily Goal
Deliberately simulate a crash between the MERGE completing and the watermark advancing, and prove — by actually running it, not just reasoning about it — that the next run recovers correctly with no data loss and no duplication.

### 2. Why Today's Work Matters
Day 15's checkpoint questions asked you to reason about crash safety in the abstract. Today, you actually break your own pipeline on purpose and watch it recover. This is the single most convincing piece of evidence you'll have — for an interviewer, or for yourself at 3am during a real incident someday — that your incremental design genuinely works, not just that it works when nothing goes wrong.

### 3. Learning Objectives
- Design and execute a deliberate failure injection test against your own pipeline
- Distinguish between "crash before the watermark advances" (safe, by design) and other failure points, and understand why they're not equally safe
- Practice writing up a failure test as a piece of documentation, the way a real incident postmortem or a resilience test report would be written

### 4. Concepts to Understand First

**Failure injection as a legitimate testing technique** — deliberately introducing a fault (killing a process mid-execution, disconnecting a network connection, as you did briefly on Day 6) to observe real recovery behavior, rather than only ever testing the happy path, is a real, named practice (chaos engineering, at larger scale, is the industry term for doing this systematically and continuously against production systems). Today's version is small and manual, but it's the same underlying idea.

**Not all crash points are equally safe, and today you'll find the actual boundary** — Day 15's design makes crashing *before* the watermark write safe (the next run correctly reprocesses and MERGE's dedup logic handles it). But what about a crash *during* the watermark write itself — is a partial write to a Delta table possible? (Answer, and today's chance to actually verify it: no — Delta's ACID guarantees, the same ones from Day 8, apply to the watermark table's writes too, since it's also a Delta table. A crash during that specific write leaves the watermark table at its previous, complete version, not a partially-written one.) Today you're not just testing your pipeline's *design* — you're indirectly re-confirming Delta's core guarantee applies uniformly across every table in your system, watermark included.

### 5. Official Documentation to Read
No new documentation today — this is an applied testing day. If you want a refresher, revisit the Delta Lake transaction log material from Day 8 with today's specific question in mind: "does this guarantee apply to *every* Delta table I write, including small metadata tables like the watermark, or only to the 'real' data tables?"

### 6. YouTube Topics to Study
- "Chaos engineering principles introduction" (conceptual grounding, not implementation — you won't build automated chaos tooling today, just borrow the mindset)

### 7. Building Tasks

**Task 1 — Design the failure injection point precisely**
- *What:* decide exactly where, in your script's execution, you'll force a crash — the cleanest approach is a temporary, clearly-commented `if os.environ.get("SIMULATE_CRASH_AFTER_MERGE") == "true": raise RuntimeError("Simulated crash for testing")` inserted immediately after Task 3's verification step from Day 15, before Task 4's watermark write
- *Why:* an environment-variable-gated crash point is safer and more controllable than actually killing the process externally (e.g., `kill -9`), and it's precise — you know exactly where execution stopped, which matters for interpreting the results correctly
- *Expected output:* a temporary, clearly-marked code addition (not something that should ever ship to `main` un-flagged — note this in a comment)
- *Verify:* re-read the placement against Day 15's Task 3/4 boundary — confirm it's genuinely between "MERGE verified" and "watermark advanced," not accidentally before verification or after the watermark write

**Task 2 — Run the job with the simulated crash triggered**
- *What:* set `SIMULATE_CRASH_AFTER_MERGE=true` and run the Silver job against a fresh batch of new data
- *Expected output:* the script crashes with the simulated error, logs should show "merge completed, verifying" and "merge verified, N rows inserted" but *not* "watermark advanced" (per Day 15's logging sequence)
- *Verify:* check the Silver Delta table directly — the new data *should* actually be present (the MERGE itself completed and committed before the simulated crash), but the watermark table should *not* reflect this batch as processed yet — this is the exact, expected "torn" state the design anticipates

**Task 3 — Inspect the "torn" state directly**
- *What:* query Silver's row count (should include the new batch) and the watermark table's value (should still show the *previous* watermark, not advanced) side by side
- *Why:* seeing this mismatch with your own eyes — data present, watermark not yet reflecting it — is what makes the abstract Day 15 checkpoint question concrete
- *Expected output:* documented evidence of exactly this state in `docs/design_decisions.md`, as a mini incident-style writeup: what was simulated, what state resulted, what's expected to happen next

**Task 4 — Run the job again, without the crash flag, and observe recovery**
- *What:* unset `SIMULATE_CRASH_AFTER_MERGE` and run the Silver job normally
- *Why:* this is the actual recovery test — since the watermark still shows the *previous* value, this run will re-read the same batch of "new" data from Bronze that was already merged into Silver last time
- *Expected output:* the job runs successfully; because the `event_key`s from that batch already exist in Silver (from the "crashed" run's MERGE, which had actually already committed), the `WHEN NOT MATCHED` clause correctly finds no new rows to insert for that overlapping range; the watermark now correctly advances
- *Verify:* Silver's row count after this recovery run is unchanged from right after the "crash" (proving no duplication occurred), and the watermark now correctly reflects the batch as processed

**Task 5 — Write up the test as a short resilience report**
- *What:* in `docs/design_decisions.md`, write a clearly-labeled "Crash Recovery Test" section: what was simulated, the exact torn state observed, the recovery run's behavior, and the conclusion (no data loss, no duplication, watermark self-corrects on the next run)
- *Why:* this write-up is itself a genuine portfolio artifact — a documented resilience test is a concrete, verifiable claim, much stronger than an unverified line in a README saying "the pipeline is idempotent and crash-safe"
- *Expected output:* a clear, incident-report-style writeup

**Task 6 — Remove the simulated crash code, or gate it clearly for future reuse**
- *What:* decide whether to delete the `SIMULATE_CRASH_AFTER_MERGE` block entirely, or keep it permanently but clearly documented as a testing-only escape hatch (with a code comment explaining its purpose) for future resilience testing after any changes to this section of the script
- *Why this design was chosen:* keeping it, clearly marked, is arguably the more mature choice — it turns today's one-off manual test into a repeatable regression test you (or a teammate) could re-run after any future change to this part of the pipeline; document your choice either way

**Task 7 — Commit**
- *Steps:* `git add spark_jobs/silver_transform.py docs/design_decisions.md && git commit -m "test: simulate and document crash-recovery behavior between merge verification and watermark advancement"`

### 8. Definition of Done
- [ ] Failure injection point precisely placed and verified
- [ ] Simulated crash run produces the expected "torn" state: data merged, watermark not yet advanced
- [ ] Torn state directly inspected and documented
- [ ] Recovery run proven to cause zero duplication and correctly advance the watermark
- [ ] Resilience test written up clearly in `docs/design_decisions.md`
- [ ] Decision made (and documented) on whether to retain the simulated-crash capability
- [ ] Commit made

### 9. Validation Steps
Re-run the entire crash-simulate-then-recover sequence a second time, against a different batch of new data, to confirm this isn't a one-off result specific to your first test's particular data.

### 10. Common Beginner Mistakes
- Placing the simulated crash point in the wrong location (before verification, or after the watermark write), which wouldn't actually test the boundary Day 15's design cares about
- Not directly inspecting the "torn" state, and only checking the final recovered state — the torn-state inspection is what makes this a real test of the *specific* claim, not just a general "does it eventually work" check
- Deleting all evidence of the test rather than writing it up — the writeup is a real portfolio artifact, don't skip it

### 11. Debugging Guide
If the recovery run in Task 4 shows *duplicated* rows rather than correctly skipping the already-merged batch, revisit your `event_key` matching condition — this would indicate the MERGE's `ON` clause isn't reliably matching rows that were already inserted, which is a serious bug worth fully understanding before moving on, since it undermines the entire crash-safety claim. If the "torn" state in Task 3 doesn't actually show the expected mismatch (e.g., the watermark somehow already advanced despite the simulated crash), check your crash-injection code is actually positioned before the watermark write in the real execution path, not just visually near it in the file.

### 12. Mentor Notes
- "What you did today — actually breaking your own system on purpose, in a controlled way, and watching it recover — is a genuinely rare thing for a portfolio project to include. Most portfolio pipelines are only ever tested against the happy path. Being able to say in an interview 'I simulated a crash at the exact boundary where data loss would be most likely, and here's the specific mechanism that made recovery safe' is a qualitatively different, stronger claim than 'the pipeline is designed to be idempotent.'"
- "The decision in Task 6 — keep the crash-injection capability as a documented, reusable testing tool, versus delete it — has no single right answer, but making the decision deliberately and writing down your reasoning is the actual skill being practiced. Real engineering teams make exactly this kind of call constantly: is this testing scaffolding worth the ongoing maintenance cost of keeping it around?"

### 13. Industry Insights
- "Chaos engineering" at companies like Netflix (originators of the Chaos Monkey tool) applies exactly this principle — deliberately injecting failures into production systems — continuously and automatically, specifically because manually reasoning about failure modes, without ever actually triggering them, reliably misses real bugs that only show up under genuine failure conditions.
- A well-written internal "resilience test" or postmortem-style document, like the one you wrote today, is often exactly the kind of artifact senior engineers are asked to produce after a real incident — practicing the format now, for a simulated rather than real failure, is genuinely useful rehearsal.

### 14. Git Workflow
- **Branch:** `feature/silver-incremental-layer`
- **Commit message:** `test: simulate and document crash-recovery behavior between merge verification and watermark advancement`
- **Merge:** not yet
- **Tag:** none today

### 15. README Updates
None yet.

### 16. Portfolio Updates
Consider highlighting this specific test in your eventual README's "Design Decisions" or "Reliability" section (Day 18 will formalize this) — it's one of the strongest, most concrete engineering claims in the whole project.

### 17. Interview Questions
- "Tell me about a time you deliberately tested a failure scenario, not just a success scenario, in something you built."
- "How would you prove, not just claim, that a pipeline is crash-safe at a specific point?"

### 18. Learning Checkpoint
- Describe the exact "torn" state you observed today, in your own words, and why it's safe.
- Why is a crash *during* the watermark table's own write not a concern, given what you know about Delta Lake?
- What's the difference between reasoning about crash safety abstractly (Day 15's checkpoint) and actually proving it (today)?

### 19. End-of-Day Checklist
- [ ] All 7 tasks complete
- [ ] Torn state directly observed and documented
- [ ] Recovery proven safe across two separate test batches
- [ ] Committed

### 20. Tomorrow's Preview
Day 17 shifts from crash-safety to data validity: implementing the Silver-layer Soda Core checks — duplicate detection, accepted values, referential integrity, and a real volume anomaly check (now genuinely possible, since you have several days of real Silver runs behind you to compare against).

---

## DAY 17 — Silver Data Quality: Duplicates, Referential Integrity, and a Real Volume Check

### 1. Daily Goal
Implement `checks/silver_checks.yml`, gating the Silver layer exactly as Bronze was gated in Week 2 — with checks that specifically target the properties Silver is responsible for guaranteeing (deduplication, business rule compliance, referential integrity) rather than repeating Bronze's schema-level checks.

### 2. Why Today's Work Matters
Bronze's Soda checks (Day 11) validated *shape*. Silver's checks need to validate the *outcomes of this week's logic* — that deduplication genuinely worked, that business rules were genuinely applied, that category parsing produced valid hierarchies. A quality check suite that just re-checks the same things at every layer isn't actually adding protection; today's checks are deliberately different in kind from Bronze's, matching what's actually new and riskier at this layer.

### 3. Learning Objectives
- Design quality checks that target layer-specific risks, not generic, repeated checks
- Implement a genuine trailing-average volume anomaly check, now that real historical run data exists
- Practice the same failure-path verification discipline from Day 11, applied to a new, more complex set of checks

### 4. Concepts to Understand First

**Why Silver's checks differ in kind from Bronze's** — Bronze already guarantees schema conformance and basic non-nullness; re-checking those exact things in Silver would be redundant, not additive. Silver's checks should specifically target what Silver's *own* logic could get wrong: did deduplication actually leave zero duplicate `event_key` values? Did the business rule filters actually remove all rows that should have been removed (e.g., is `price >= 0` genuinely true for every remaining row, not just assumed true because the filter code exists)? Does every row have a non-null `category_l1` (given your Day 13 decision that at least the first level should always be parseable)? Each of these checks is validating this week's *specific new logic*, not re-validating Bronze's already-covered concerns.

**A genuine trailing-average volume check, now possible** — Day 11 explicitly and honestly deferred a real trailing-average volume check because you didn't yet have multiple days of run history to compare against. By today, across Week 2 and Week 3's testing, you've actually run Bronze and Silver several times, on different days, with different batches. This is enough real history to implement a genuine (if still small-sample) trailing comparison — document this evolution explicitly, closing the loop on Day 11's honest scoping decision.

### 5. Official Documentation to Read
1. Soda Core checks reference — the sections you didn't need in Week 2: `duplicate_count`, `valid values` with a list, and a custom SQL-based check (for the referential/category-level check, which doesn't map neatly to a built-in check type)

### 6. YouTube Topics to Study
- "Soda Core custom SQL checks"
- "Data quality trailing average anomaly detection concept"

### 7. Building Tasks

**Task 1 — Write the duplicate detection check**
- *What:* `duplicate_count(event_key) = 0` in `checks/silver_checks.yml`
- *Why:* this is the single most important Silver-specific check — it directly validates that this week's MERGE-based deduplication logic is actually working in the real, current state of the table, not just in the isolated tests from Days 14–16
- *Expected output:* passing check against real Silver data
- *Verify:* run the scan, confirm pass

**Task 2 — Write accepted-values checks for business rule outcomes**
- *What:* `valid values for price >= 0` (or the Soda equivalent syntax for a numeric range/expression check) and confirm no null `user_session` values remain, directly validating Day 14's filter logic actually took effect in the real table, not just in a DataFrame you inspected once during development
- *Expected output:* passing checks
- *Verify:* deliberately think through whether these checks could pass "by accident" (e.g., if the filter code had a bug but happened not to trigger on this particular dataset) — if you're not confident, that's a sign to also spot check a few real rows manually, the same skepticism from Day 11's Task 6

**Task 3 — Write a custom SQL check for category hierarchy validity**
- *What:* using Soda Core's custom SQL check capability, write a check asserting the count of rows with a null `category_l1` is zero (per Day 13's decision that level 1 should always be parseable)
- *Why:* this directly validates a specific piece of Day 14's parsing logic that doesn't map to one of Soda's simple built-in check types, which is exactly when a custom SQL check is the right tool
- *Expected output:* a working custom check, passing against real data

**Task 4 — Implement the real trailing-average volume check**
- *What:* using your accumulated real run history (row counts from your `audit`-style logging or simply the Silver table's own `event_date` distribution), write a check comparing the most recent batch's row count against a trailing average of prior batches, with a reasonable tolerance band (e.g., ±30%, matching the tolerance style suggested in the Implementation Guide's Soda prompt from Section 11)
- *Why:* this closes the loop on Day 11's honestly-deferred simplification — document explicitly, in `docs/design_decisions.md`, that this check has now evolved from "row_count > 0" to a genuine trailing comparison, and why today specifically was the right time (enough real history now exists)
- *Expected output:* a working trailing-average check, passing against your real, if still small, run history
- *Verify:* the check's logic makes sense when you reason through it by hand against your actual known batch sizes from this week

**Task 5 — Deliberately inject a failure for each new check category**
- *What:* exactly as on Day 11, write small, deliberately-bad test batches (a duplicate `event_key`, a negative price that somehow bypassed the filter, a null `category_l1`) into a *test copy* of Silver, and confirm each corresponding check fails with a non-zero exit code
- *Why:* the same non-negotiable discipline from Day 11 and every prior "test the failure path" task this project — a check you haven't watched fail is not a verified check
- *Expected output:* three separate, confirmed failure detections, one per new check category
- *Verify:* `echo $?` non-zero for each, and the Soda scan output clearly identifies *which* check failed and why, for each case

**Task 6 — Commit**
- *Steps:* `git add checks/silver_checks.yml docs/design_decisions.md && git commit -m "feat: implement Silver-layer Soda checks (duplicates, business rules, category validity, real trailing-average volume check), verify all failure paths"`

### 8. Definition of Done
- [ ] `checks/silver_checks.yml` implements duplicate detection, business rule validation, custom category-hierarchy check, and a genuine trailing-average volume check
- [ ] All checks pass against real, current Silver data
- [ ] Each check's failure path deliberately triggered and confirmed with a non-zero exit code
- [ ] Day 11's honest simplification explicitly closed out and documented
- [ ] Commit made

### 9. Validation Steps
Run the full Silver Soda scan twice in a row against real data to confirm stable, non-flaky results, exactly as you did for Bronze on Day 11.

### 10. Common Beginner Mistakes
- Re-implementing Bronze's schema/null checks redundantly at the Silver layer instead of writing checks specific to Silver's own logic
- Trusting that business-rule checks pass "because the filter code exists," without actually verifying against real current data
- Forgetting to close the loop on Day 11's deferred volume-check simplification, leaving an inconsistency between what the code comment said ("evolve this once you have history") and what you actually did

### 11. Debugging Guide
If the custom SQL check for category hierarchy behaves unexpectedly, run the exact same SQL directly against your Silver table outside of Soda first (in a plain Spark SQL or DuckDB query) to isolate whether the issue is your SQL logic or Soda's custom-check wiring specifically — the same "isolate the underlying capability from the tool wrapping it" principle from Day 11's DuckDB-Delta troubleshooting.

### 12. Mentor Notes
- "Notice that today's checks are deliberately *not* a copy-paste of Bronze's checks with the table name swapped. A quality framework that just repeats the same generic checks at every layer gives a false sense of thoroughness without actually adding protection. Real platform teams think hard about what's genuinely new or risky at each stage, and that's exactly the exercise you just did."
- "Closing the loop on Day 11's deferred volume check is a small but real example of a broader engineering discipline: technical debt you take on deliberately, with a documented plan for when and how to pay it down, is completely different from technical debt you just forget about. You did the former, correctly."

### 13. Industry Insights
- Data quality frameworks that mature over time — starting with achievable checks and deliberately evolving them as more operational history accumulates, exactly as you did with the volume check — are the realistic norm; teams that try to build a "complete" quality framework on day one, before they have real operational data to calibrate thresholds against, often end up with checks that are either too loose to catch real problems or so strict they generate constant false alarms that get ignored.
- Custom SQL checks (Task 3) are a common escape hatch in every quality framework, Soda Core included — knowing when to reach for a built-in check type versus write custom SQL is itself a design skill worth being able to discuss.

### 14. Git Workflow
- **Branch:** `feature/silver-incremental-layer`
- **Commit message:** `feat: implement Silver-layer Soda checks (duplicates, business rules, category validity, real trailing-average volume check), verify all failure paths`
- **Merge:** not yet — merges tomorrow after Day 18's full Phase 3 validation
- **Tag:** none today

### 15. README Updates
None yet.

### 16. Portfolio Updates
None yet.

### 17. Interview Questions
- "How do you decide what quality checks belong at which layer of a medallion architecture, rather than just repeating the same checks everywhere?"
- "When would you write a custom SQL data quality check instead of using a framework's built-in check types?"

### 18. Learning Checkpoint
- Why are Silver's checks deliberately different in kind from Bronze's, rather than a repeat of them?
- What specifically does the duplicate-count check validate that nothing in Bronze's checks could have caught?
- How did today's volume check evolve from Day 11's honestly-simplified version, and why was today the right time to make that change?

### 19. End-of-Day Checklist
- [ ] All 6 tasks complete
- [ ] All four new check categories' failure paths verified
- [ ] Committed on `feature/silver-incremental-layer`

### 20. Tomorrow's Preview
Day 18 closes out Phase 3 entirely — the same rigorous cold-start validation, self-review, merge, tag, and README/portfolio update discipline from Week 1's Day 7 and Week 2's Day 12, now applied to the full ingestion → Bronze → Silver → dual quality gates chain.

---

## DAY 18 — Phase 3 Completion: Full Cold-Start Validation, Merge, Portfolio Checkpoint

### 1. Daily Goal
Prove the entire chain — ingestion, Bronze, Bronze quality gate, Silver, Silver quality gate — works end-to-end from a genuinely cold start, merge `feature/silver-incremental-layer` into `main`, and bring documentation and portfolio artifacts fully up to date.

### 2. Why Today's Work Matters
This is now the third time you've done this exact discipline (Week 1 Day 7, Week 2 Day 12), and the chain under test keeps growing. That's deliberate — each cold-start validation is more convincing than the last, because it's proving reproducibility across a genuinely more complex, more realistic system. This is also the day the crash-recovery test from Day 16 gets folded into your permanent documentation as a real, referenceable engineering artifact.

### 3. Learning Objectives
- Apply the now-familiar cold-start validation discipline to a five-step chain with two internal quality gates
- Practice writing documentation that makes a resilience claim (crash safety) verifiable, not just asserted
- Continue the habit of an honestly-scoped, currently-accurate architecture diagram

### 4. Concepts to Understand First
No new concepts today — as with Day 7 and Day 12, this is a consolidation and validation day. The one thing worth naming explicitly: today's cold-start test needs to include a version of Day 16's crash-recovery test as part of the standard validation, not just the "happy path" chain — since crash-safety is now a claim your project actually makes, your validation routine should actually check it, not just the parts that were easy to verify.

### 5. Official Documentation to Read
None new — reconcile `docs/design_decisions.md` entries from Days 15–17 against the final implementation, exactly as in prior week-closing days.

### 6. YouTube Topics to Study
None required today.

### 7. Building Tasks

**Task 1 — Full cold-start test: ingestion → Bronze → Bronze Soda gate → Silver → Silver Soda gate**
- *What:* `docker compose down -v && docker compose up -d`; recreate buckets; run the full chain in order
- *Expected output:* all five steps succeed with no manual intervention
- *Verify:* final Silver Soda scan passes; `DESCRIBE HISTORY` on both Bronze and Silver show clean, expected version sequences with no leftover state from earlier development
- *Common mistake:* discovering an implicit dependency on the watermark table's state from earlier testing that a true cold start would not have — if found, fix the root cause (make sure the missing-watermark edge case from Day 14 genuinely handles this)

**Task 2 — Re-run the crash-recovery test from Day 16 as part of this validation**
- *What:* using the retained (per your Day 16 decision) `SIMULATE_CRASH_AFTER_MERGE` capability, or a fresh manual repeat if you chose to delete it, re-confirm the crash-safety claim holds against this now-more-complete version of the pipeline
- *Why:* code has changed since Day 16 (Day 17's quality checks were added); re-confirming the crash-safety claim still holds after those changes is exactly the kind of regression check the retained testing capability was meant to enable
- *Expected output:* same safe recovery behavior as Day 16
- *Verify:* zero duplication, correct watermark self-correction, exactly as before

**Task 3 — Self-review both Silver-related files as if reviewing a colleague's PR**
- *What:* read `silver_transform.py` and `checks/silver_checks.yml` fully, checking for dead code, leftover debug/test artifacts (especially anything related to the crash simulation that shouldn't ship un-flagged), consistency with the established house style
- *Expected output:* cleanup list, then fixes applied

**Task 4 — Merge and tag**
- *Steps:* merge `feature/silver-incremental-layer` into `main`, delete the branch, `git tag v0.4-phase3-complete && git push --tags`

**Task 5 — Update the README**
- *What:* mark Phase 3 complete; expand run instructions to include Silver and its quality gate; update the architecture note to Raw → Bronze (Delta, quality-gated) → Silver (Delta, incremental MERGE, quality-gated); add a short, clearly-labeled "Reliability" subsection referencing the crash-recovery test from Day 16 with a one-paragraph, plain-language summary of what was proven — this is one of your strongest, most concrete engineering claims in the whole project, and it deserves real visibility in the README, not just a buried note in `docs/design_decisions.md`
- *Verify:* someone cloning the repo fresh could reproduce the full chain, including understanding what crash-safety guarantee is claimed and why

**Task 6 — Update the architecture diagram**
- *What:* extend the diagram from Day 12 (Raw → Bronze) to now show Silver, including its quality gate, matching current real state exactly

**Task 7 — Portfolio screenshots**
- *What:* screenshot the Silver Delta table's `DESCRIBE HISTORY` output showing a clean MERGE-based version sequence, and the passing Silver Soda scan output
- *Expected output:* both saved to `docs/`

### 8. Definition of Done
- [ ] Full five-step cold-start chain succeeds with zero manual fixes
- [ ] Crash-recovery test re-confirmed against the latest code
- [ ] Self-review completed, cleanup applied
- [ ] `feature/silver-incremental-layer` merged into `main`, tagged `v0.4-phase3-complete`
- [ ] README updated: Phase 3 progress, run instructions, updated architecture note, new Reliability subsection
- [ ] Architecture diagram updated to reflect Silver
- [ ] Portfolio screenshots saved

### 9. Validation Steps
Same honest bar as every prior week-closing day: could someone else, with only your README and a valid `.env`, reproduce the full chain and understand exactly what reliability guarantees your pipeline claims and why? If the crash-safety claim in your README isn't specific and verifiable-sounding (rather than a vague "the pipeline is designed to be resilient"), revise it until it is.

### 10. Common Beginner Mistakes
- Skipping the crash-recovery re-test because "it already passed on Day 16" — code has changed since then, and this is exactly the kind of regression this retained test exists to catch
- Writing a vague, unverifiable README claim about reliability instead of a specific, grounded one referencing the actual test performed
- Letting the architecture diagram fall behind reality again

### 11. Debugging Guide
If the cold-start chain fails specifically at Silver's first-run table-creation path (Day 15's Task 1), double check this path is genuinely exercised by a true cold start — it's easy to have only ever tested it once, early on, and then never again as later testing always found the table already existing.

### 12. Mentor Notes
- "The Reliability subsection you're adding to the README today is worth taking seriously as a piece of writing, not just a checkbox. 'I simulated a process crash at the exact point where data loss would be most likely and verified recovery caused zero duplication and zero loss' is a sentence that reads completely differently to an experienced engineer than 'the pipeline is idempotent.' Specificity is what makes a reliability claim credible."
- "You've now closed out three consecutive weeks with the exact same rigor: cold-start test, self-review, merge, tag, honest documentation update. This is genuinely what a disciplined engineering habit looks like — not enthusiasm on day one that fades, but the same bar applied consistently, week after week, even when it would be tempting to skip a step because 'it probably still works.'"

### 13. Industry Insights
- README "Reliability" or "Design Decisions" sections that reference specific, performed tests (rather than general claims) are relatively rare in portfolio projects and genuinely stand out to technical reviewers — this is a differentiator worth leaning into explicitly when you get to the Implementation Guide's portfolio-optimization advice in later weeks.

### 14. Git Workflow
- **Branch:** `main` (post-merge)
- **Commit/merge:** `feature/silver-incremental-layer` → `main`
- **Tag:** `v0.4-phase3-complete`

### 15. README Updates
Completed as Task 5 above.

### 16. Portfolio Updates
Two new screenshots, updated architecture diagram, new Reliability subsection — all reflecting genuine, current, tested state.

### 17. Interview Questions
- "Walk me through your project's reliability story — what have you actually tested, versus what you're assuming works?"
- "How did your architecture diagram evolve over the course of building this, and why did you keep it in sync with actual progress rather than drawing the full target architecture upfront?"

### 18. Learning Checkpoint
- Why did today's cold-start validation need to include a re-run of the crash-recovery test, specifically, rather than just the happy-path chain?
- What makes your README's new Reliability claim specific and verifiable, rather than vague?

### 19. End-of-Day Checklist
- [ ] All 7 tasks complete
- [ ] Crash-recovery re-verified against current code
- [ ] Merged, tagged, README/diagram/portfolio all updated

### 20. Tomorrow's Preview
Day 19 begins Phase 4: formalizing the Raw-layer quality checks you haven't yet written (Bronze and Silver exist; Raw doesn't yet), and starting to design a single, reusable script that runs all three check suites together as one coherent quality gate — the artifact that Airflow will eventually call directly.

---

## DAY 19 — Data Quality Framework, Part 1: Raw-Layer Checks & the Missing Suite

### 1. Daily Goal
Write `checks/raw_checks.yml` — the one check suite from the Implementation Guide's three-layer quality design (Section 1, Section 4) that hasn't been built yet — and reconcile all three suites' configuration into one consistent, reusable setup.

### 2. Why Today's Work Matters
Weeks 2 and 3 built real, working quality gates at Bronze and Silver, but the Implementation Guide's Phase 4 specifically calls for a check immediately after **Raw ingestion** too — catching problems at the earliest possible point, before they ever reach Bronze's schema enforcement. Today closes that gap, and in doing so, forces you to think about what's actually worth checking at the rawest, least-processed stage of your pipeline.

### 3. Learning Objectives
- Design quality checks appropriate for genuinely raw, unprocessed data — different in kind again from both Bronze's and Silver's checks
- Reconcile three independently-built check suites into one consistent `checks/` configuration setup
- Practice thinking about "how early can a real problem be caught" as a design question in its own right

### 4. Concepts to Understand First

**What's worth checking at the Raw layer, given it hasn't been schema-enforced yet** — Raw data, straight from your Week 1 ingestion service, hasn't been through Bronze's schema casting yet, so you can't meaningfully check specific typed field values the way Bronze and Silver's checks do. What you *can* check: does the expected file/partition structure exist (the `source_file=/ingested_date=` layout from Week 1's ingestion design)? Is the raw file non-empty? Does a basic row count exist at all? These are structural/existence checks, appropriate to how little processing has happened yet — deliberately more minimal than Bronze's, and that's the correct design, not an oversight.

**Why catching a problem at Raw is more valuable than catching the same underlying problem at Bronze** — if your Kaggle source ever changed its file format entirely (a genuinely realistic risk for any third-party data dependency), a Raw-layer structural check would fail immediately, clearly pointing at "the ingestion output doesn't look like the source data anymore" — versus discovering the same underlying problem only when Bronze's schema enforcement rejects nearly every row, which is a more confusing symptom to debug backward from. This is the concrete value of "shift quality checks left" (closer to the source), which is the actual reasoning the Implementation Guide's Section 0 review was gesturing at when it criticized v1.0's checks running "too late."

**Reconciling three independently-evolved configs** — Bronze's checks (Day 11) were your first Soda Core setup; Silver's (Day 17) reused and extended that pattern days later. Today, before adding a third suite, is a natural point to check: is your `checks/configuration.yml` connection setup genuinely shared and consistent across all three, or did small inconsistencies creep in between Day 11 and Day 17 that are worth cleaning up now, before a third suite makes any inconsistency more annoying to fix later?

### 5. Official Documentation to Read
1. Soda Core docs — re-skim the "Checks" reference specifically for structural/existence-oriented checks (e.g., checking a table/dataset simply has rows, or file-count-style checks if your Raw data is registered as a scannable dataset) — you may find Raw-layer checks in this project are better implemented as a lightweight custom Python script (using `boto3` to inspect MinIO directly) rather than forced into Soda Core's YAML format if the data isn't yet in a tabular form Soda can easily connect to; decide which approach fits better and document why

### 6. YouTube Topics to Study
- "Data quality checks shift left explained" (the general industry concept behind today's Raw-layer reasoning)

### 7. Building Tasks

**Task 1 — Decide Raw checks' implementation approach: Soda YAML vs. lightweight custom script**
- *What:* since Raw data isn't in a clean tabular Delta/DuckDB-queryable form the way Bronze/Silver are, evaluate whether Soda Core can connect to it reasonably (e.g., via DuckDB reading the raw CSVs directly) or whether a small, purpose-built Python script using `boto3` (checking object existence, sizes, and basic row counts by streaming/sampling the file) is the more honest, simpler choice
- *Why:* forcing every check into one tool's format regardless of fit is a common over-engineering trap; choosing the right tool for this specific, more minimal checking need is a real design decision worth making deliberately and documenting
- *Expected output:* a decision, documented in `docs/design_decisions.md`, with reasoning
- *Verify:* re-read your reasoning — would you make the same call if asked to defend it in an interview?

**Task 2 — Implement the chosen approach for structural existence checks**
- *What:* whichever path you chose, implement checks for: expected partition structure exists for the most recent ingestion run, the raw file(s) are non-empty (size > 0), and a basic parseable row count is achievable (even a rough line count for CSVs)
- *Expected output:* a working check (Soda YAML or a small script) that passes against your real, current Raw data
- *Verify:* run it, confirm pass

**Task 3 — Implement a basic freshness check for Raw, mirroring Bronze's but at the earliest point**
- *What:* check that the most recent `ingested_date` partition is recent enough (reusing similar reasoning to Day 11's Bronze freshness check, but now catching a stalled *ingestion* specifically, even earlier than a stalled Bronze run would be caught)
- *Expected output:* a passing check
- *Verify:* confirm the specific failure mode this catches is genuinely distinct from Bronze's freshness check (a stalled ingestion job vs. a stalled Bronze job — related but not identical problems, worth being able to articulate the difference)

**Task 4 — Deliberately break the Raw checks and confirm failure detection**
- *What:* same non-negotiable discipline as every prior quality check task — simulate a missing partition (temporarily rename/hide the expected raw data path) and confirm your check correctly fails with a clear, actionable message
- *Expected output:* confirmed failure detection
- *Verify:* non-zero exit code (if scripted) or clear failure output

**Task 5 — Reconcile the three suites' configuration**
- *What:* review `checks/configuration.yml` and the general structure of `raw_checks.yml`/`bronze_checks.yml`/`silver_checks.yml` together; fix any naming or structural inconsistencies that crept in across the three days they were each written on
- *Expected output:* a consistent, clean `checks/` directory that reads as one coherent framework, not three loosely related files
- *Verify:* re-read all three suites back to back — do they feel like they were designed together, even though they were built on different days?

**Task 6 — Commit**
- *Steps:* `git checkout -b feature/data-quality-framework && git add checks/ docs/design_decisions.md && git commit -m "feat: implement Raw-layer quality checks, reconcile three-layer check suite configuration"`

### 8. Definition of Done
- [ ] Raw-layer checks implemented (Soda YAML or purpose-built script, deliberately chosen and documented)
- [ ] Structural existence, non-empty, and freshness checks all implemented and passing against real Raw data
- [ ] Failure path deliberately triggered and confirmed
- [ ] All three check suites' configuration reconciled into a consistent whole
- [ ] New feature branch created and committed

### 9. Validation Steps
Run all three suites (Raw, Bronze, Silver) back to back against your currently cold-start-validated real data, confirming all pass — this previews tomorrow's work of formally combining them into one gate script.

### 10. Common Beginner Mistakes
- Forcing Raw checks into Soda's YAML format when the data genuinely isn't in a good shape for it yet, rather than making a deliberate tool choice
- Writing Raw checks that duplicate what Bronze's schema enforcement already effectively checks, rather than focusing on genuinely-Raw-appropriate structural concerns
- Skipping the reconciliation task and leaving three suites that don't read as a coherent whole

### 11. Debugging Guide
If a `boto3`-based existence check behaves inconsistently, double check you're using the same `MINIO_ENDPOINT`-aware configuration pattern established back on Day 6's ingestion script (host vs. in-container endpoint differences are a recurring theme worth being alert to any time you write new MinIO-touching code).

### 12. Mentor Notes
- "The decision in Task 1 — whether to force Raw checks into Soda's format or write a small purpose-built script instead — is a genuinely good example of avoiding tool-fundamentalism. Using Soda Core everywhere because it's your chosen framework, even where it's an awkward fit, would be worse engineering than recognizing where a five-line boto3 script is honestly the better tool for a specific, minimal need."
- "'Shift quality checks left' — catching problems as early as possible in a pipeline — is a genuinely important principle, and today's Raw checks are a real, concrete instance of applying it, not just a checkbox from the Implementation Guide's phase list. Being able to explain *why* a Raw-layer freshness check catches a meaningfully different failure than Bronze's freshness check shows you understood the principle, not just implemented a requirement."

### 13. Industry Insights
- "Shift left" is a term borrowed from software testing generally (catching bugs earlier in the development process, e.g., via linting and unit tests, rather than only in production) and applies directly to data quality — the earlier a real, mature data platform can detect a source-data problem, the smaller the blast radius before it's caught.
- Being pragmatic about tool fit (Task 1's decision) rather than dogmatically using one framework for everything is a trait senior engineers specifically look for when reviewing junior engineers' design decisions — over-engineering a simple check into an ill-fitting framework is a common junior-engineer pattern worth having consciously avoided today.

### 14. Git Workflow
- **Branch:** `feature/data-quality-framework` (new)
- **Commit message:** `feat: implement Raw-layer quality checks, reconcile three-layer check suite configuration`
- **Merge:** not yet
- **Tag:** none today

### 15. README Updates
None yet — Day 21 covers the full Phase 4 README update.

### 16. Portfolio Updates
None yet.

### 17. Interview Questions
- "What's the 'shift left' principle in data quality, and can you give a concrete example from your own project?"
- "How do you decide which tool is the right fit for a specific data quality check, rather than defaulting to whatever framework you're already using?"

### 18. Learning Checkpoint
- Why are Raw-layer checks structurally different from Bronze's, given how little processing has happened by that point?
- What specific failure mode does a Raw-layer freshness check catch that a Bronze-layer freshness check wouldn't catch as early?
- What did you decide about Soda YAML vs. a custom script for Raw checks, and why?

### 19. End-of-Day Checklist
- [ ] All 6 tasks complete
- [ ] Raw check failure path verified
- [ ] Three-suite configuration reconciled and re-read as a coherent whole
- [ ] Committed on new feature branch

### 20. Tomorrow's Preview
Day 20 builds the single aggregating gate script (`checks/run_quality_gate.py`) that runs all three suites in the correct pipeline order, collects their results into one audit-ready report, and produces a single, unambiguous pass/fail signal — the exact artifact Airflow will call directly once orchestration begins in a future week.

---

## DAY 20 — Building the Unified Quality Gate Runner

### 1. Daily Goal
Build `checks/run_quality_gate.py`: a single script that runs the Raw, Bronze, and Silver Soda scans in sequence, aggregates their results into one structured report, and exits with a single, clear pass/fail signal representing the whole framework.

### 2. Why Today's Work Matters
Right now, "run the quality framework" means manually running three separate commands and mentally tracking whether each passed. That doesn't scale to Airflow, and it doesn't scale to your own daily workflow either. Today's script is the single artifact that turns three independent check suites into one coherent, callable quality gate — precisely the shape Section 7 of the Implementation Guide describes for how `dag_data_quality` will eventually call into this work.

### 3. Learning Objectives
- Design and implement a script that orchestrates multiple subprocess-style tool invocations (Soda scans) and aggregates their exit codes and output correctly
- Produce a single, audit-ready report artifact from multiple independent check runs
- Understand the specific design requirement that a *partial* failure (e.g., Bronze passes, Silver fails) must still produce an overall failure signal — not be silently averaged away or lost

### 4. Concepts to Understand First

**Aggregating multiple exit codes into one overall signal, correctly** — if you run three subprocesses and simply return the *last* one's exit code, a failure in an earlier suite (Raw or Bronze) could be silently masked by a later suite passing. The correct approach: track each suite's result independently, and the overall script's exit code should be non-zero if *any* suite failed — this sounds obvious stated directly, but it's an easy mistake to make when translating "run three things in sequence" into code without thinking carefully about aggregation.

**Should a failed suite stop the remaining suites, or should all three always run regardless?** — this is a genuine design decision, not an obvious default. Running all three regardless of earlier failures gives you a complete picture of everything wrong in one report (useful for debugging). Stopping at the first failure is faster and matches "fail fast" thinking. Given this project's Section 7 design (a Raw failure conceptually shouldn't block you from also knowing whether Bronze/Silver, if they ran against already-existing data, are independently healthy), decide deliberately which behavior fits best here, and document your reasoning — there's a real, defensible case for either choice.

**A structured, aggregated report, not just three separate outputs pasted together** — today's script should produce one clear artifact (a JSON or well-formatted text summary) showing, at a glance: which suites ran, which passed/failed, and for any failures, enough detail to know where to look next — this is the artifact a future Airflow task's logs would show, and the artifact your own future self will actually read when deciding whether to trust a given day's pipeline run.

### 5. Official Documentation to Read
1. Python `subprocess` module docs — `subprocess.run()`, capturing `returncode` and output, specifically
2. Python `json` module — just enough to structure a clean aggregated report if you choose JSON output

### 6. YouTube Topics to Study
- "Python subprocess run capture output returncode"

### 7. Building Tasks

**Task 1 — Design the aggregation and stop-vs-continue behavior, in writing, before coding**
- *What:* decide and document: do all three suites always run regardless of earlier failures (recommended, per the reasoning above, for this project's use case), and how exactly will the overall exit code be computed from three individual results
- *Expected output:* a short design note in `docs/design_decisions.md`
- *Verify:* the same "could someone else implement this from my notes" bar as every prior design task this project

**Task 2 — Implement the script skeleton with the established house style**
- *What:* argparse (perhaps a `--layer` flag to optionally run just one suite during development, defaulting to "all"), structured logging matching your other scripts
- *Expected output:* runs with `--help`

**Task 3 — Implement running each Soda suite as a subprocess and capturing its result**
- *What:* for each of Raw, Bronze, Silver (in that fixed order, matching pipeline order), invoke the Soda scan command via `subprocess.run()`, capturing `returncode`, `stdout`, and `stderr`
- *Expected output:* a data structure (e.g., a list of dicts) holding each suite's name, exit code, and captured output
- *Verify:* run against your currently-passing real data and confirm all three captured results show success

**Task 4 — Implement correct overall aggregation**
- *What:* per Task 1's design, compute the overall pass/fail as "did *any* suite fail," not "did the last suite fail"
- *Expected output:* the script's own exit code correctly reflects the aggregate
- *Verify:* deliberately make just the *first* suite (Raw) fail (using Day 19's failure-injection approach) while leaving Bronze/Silver passing, and confirm the overall script still exits non-zero — this is the exact "don't let a later pass mask an earlier failure" bug this task exists to prevent

**Task 5 — Implement the structured aggregate report output**
- *What:* write a clean JSON (or well-formatted text) report to `logs/quality_gate_report_{timestamp}.json` (or similar), showing each suite's pass/fail status and key details for any failures
- *Expected output:* a genuinely useful report file
- *Verify:* read a real report file — could you tell, in 10 seconds, exactly which suite(s) failed and roughly why, without re-running anything?

**Task 6 — Full validation run against real data**
- *What:* run `checks/run_quality_gate.py` against your fully cold-start-validated real pipeline state
- *Expected output:* overall pass, clean report, all three suites reflected accurately

**Task 7 — Re-run Day 19's Raw-failure test and Day 11/17's Bronze/Silver failure tests through the unified gate, one at a time**
- *What:* confirm the unified gate correctly surfaces each previously-tested failure mode, not just in isolation as before, but through this new aggregating script specifically
- *Why:* this proves the aggregation script itself is correct, not just that the underlying Soda suites are — it's possible to build a perfectly good set of checks and then introduce a new bug purely in the aggregation logic wrapping them, which is exactly what today's tasks are testing for
- *Expected output:* three separate confirmed failure detections, correctly surfaced through the unified script

**Task 8 — Commit**
- *Steps:* `git add checks/run_quality_gate.py docs/design_decisions.md && git commit -m "feat: build unified quality gate runner aggregating Raw/Bronze/Silver Soda scans with correct multi-suite failure aggregation"`

### 8. Definition of Done
- [ ] Design decision on stop-vs-continue and aggregation logic documented before implementation
- [ ] `checks/run_quality_gate.py` runs all three suites in correct pipeline order
- [ ] Overall exit code correctly reflects "any suite failed," verified specifically against an early-suite-fails-later-suites-pass scenario
- [ ] Structured, genuinely readable aggregate report produced
- [ ] All three previously-tested failure modes (Raw, Bronze, Silver) re-verified as correctly surfaced through this new unified script specifically
- [ ] Commit made

### 9. Validation Steps
Run the full gate script three times in a row against stable real data, confirming consistent, non-flaky pass results and identical report structure each time.

### 10. Common Beginner Mistakes
- Returning only the last suite's exit code, silently masking earlier failures
- Not specifically re-testing each already-known failure mode through the new aggregation layer, assuming "the underlying checks already work" is sufficient proof the wrapper is also correct
- Producing a report that requires re-running things or reading raw Soda output to actually understand what failed

### 11. Debugging Guide
If the overall exit code doesn't reflect an early failure correctly, print each individual suite's captured `returncode` explicitly before computing the aggregate, and step through your aggregation logic by hand against that printed evidence — this is a good moment to add a temporary debug print, confirm the fix, then remove it, exactly the disciplined debugging habit from earlier weeks.

### 12. Mentor Notes
- "Task 4's specific test — an early suite failing while later ones pass — is the kind of test that separates 'I wrote aggregation logic' from 'I proved my aggregation logic is actually correct.' The bug this catches (a later pass silently overwriting an earlier fail signal) is genuinely common and easy to introduce without noticing, especially under time pressure. Building the habit of testing your aggregation logic specifically, not just your individual components, will serve you well any time you're combining multiple check/test results into one signal — which comes up constantly in real CI/CD and orchestration work."
- "The report file from Task 5 is worth taking seriously as a piece of writing, exactly like Day 18's README Reliability section. A report that requires you to already know what went wrong in order to interpret it isn't actually doing its job. Write it as if a tired version of yourself, or a teammate who's never seen this code, needs to understand the situation from the report alone."

### 13. Industry Insights
- This exact pattern — running multiple independent checks/tests and correctly aggregating pass/fail signals without masking — is precisely what CI/CD systems (GitHub Actions, Jenkins, CircleCI) do at a larger scale when a pipeline has multiple parallel or sequential test jobs; understanding the aggregation logic yourself, at small scale, makes reasoning about "why did my CI pipeline report green when one job actually failed" bugs in real tooling much more intuitive later.
- A single, unified quality gate script exposing one clear command and one clear signal is exactly the kind of interface an orchestrator (Airflow, in a future week) wants to call — designing this interface *before* Airflow exists, rather than building three separate Airflow tasks that each call Soda directly, keeps your orchestration layer simpler and your quality logic testable independently of Airflow, which is a genuinely good architectural instinct to practice.

### 14. Git Workflow
- **Branch:** `feature/data-quality-framework`
- **Commit message:** `feat: build unified quality gate runner aggregating Raw/Bronze/Silver Soda scans with correct multi-suite failure aggregation`
- **Merge:** not yet — merges tomorrow after Day 21's full validation
- **Tag:** none today

### 15. README Updates
None yet.

### 16. Portfolio Updates
None yet.

### 17. Interview Questions
- "How would you aggregate the results of multiple independent test or check runs into one overall pass/fail signal, and what's the most common mistake people make doing this?"
- "Why might you build a standalone quality gate script rather than wiring three separate checks directly into your orchestrator?"

### 18. Learning Checkpoint
- Walk through exactly how your script computes its overall exit code from three individual suite results, and explain the specific bug this design avoids.
- Why did you (or didn't you) choose to stop running remaining suites after an early failure, and what's the tradeoff either way?
- Why is testing the aggregation logic itself, separately from testing that the underlying checks work, a meaningfully different and necessary test?

### 19. End-of-Day Checklist
- [ ] All 8 tasks complete
- [ ] Early-failure-masking scenario specifically tested and proven not to occur
- [ ] All three known failure modes re-verified through the unified script
- [ ] Committed on `feature/data-quality-framework`

### 20. Tomorrow's Preview
Day 21 closes out Phase 4 and Week 3 entirely — full cold-start validation of the complete pipeline plus unified quality gate, merge, tag, and a genuinely substantial README/portfolio update, since the project now has a complete, working, three-layer, professionally-documented data quality framework to show for it.

---

## DAY 21 — Phase 4 Completion: Full Validation, Merge, and Week 3 Portfolio Checkpoint

### 1. Daily Goal
Validate the complete pipeline — ingestion through Silver, gated by the unified quality framework — from a genuine cold start, merge `feature/data-quality-framework` into `main`, and update all documentation and portfolio artifacts to reflect a project that now has a fully working, professionally-structured Bronze/Silver Delta Lake layer with comprehensive, tested data quality gating.

### 2. Why Today's Work Matters
This closes out not just Phase 4, but the entire "ingestion through validated Silver" arc that's spanned three weeks. Today's cold-start test is the most comprehensive one yet, and today's README/portfolio update represents the first genuinely substantial, interview-ready milestone in this project — everything before Gold-layer analytics and dashboards, but a complete, working, production-patterned data engineering pipeline in its own right.

### 3. Learning Objectives
- Apply the cold-start validation discipline to the most complete version of the pipeline yet
- Practice writing a milestone-level README/portfolio update, not just an incremental one
- Consolidate three weeks of design decisions into documentation someone could actually learn from by reading it

### 4. Concepts to Understand First
No new concepts — today is the largest-scope application yet of validation and documentation discipline you've now practiced four times (Days 7, 12, 18, and today). The one thing worth being deliberate about: today's README/portfolio update should read differently in *scale*, not just content, from the incremental updates on Days 7/12/18 — this is a natural point to also do a first pass at the "why this project, why these choices" narrative framing the Implementation Guide's Section 12 portfolio-optimization advice describes, since you now have enough real, tested engineering decisions behind you to make that narrative genuinely substantive rather than aspirational.

### 5. Official Documentation to Read
None new — re-read the Implementation Guide's Section 12 (Portfolio Optimization) today specifically, now that you have real work to apply its advice to, rather than reading it as abstract guidance before you had anything to show.

### 6. YouTube Topics to Study
None required today.

### 7. Building Tasks

**Task 1 — Full cold-start validation of the complete chain plus unified gate**
- *What:* `docker compose down -v && docker compose up -d`; recreate buckets; run ingestion → Bronze → Silver → `run_quality_gate.py`
- *Expected output:* the unified gate reports overall success, with a clean report file
- *Verify:* every suite shows passing in the aggregated report; `DESCRIBE HISTORY` on both Delta tables shows clean, expected version sequences

**Task 2 — Re-run the crash-recovery test one final time through the complete current codebase**
- *What:* same discipline as Day 18, now against the fully current state including the quality gate script
- *Expected output:* same safe recovery behavior
- *Verify:* zero duplication, correct watermark self-correction

**Task 3 — Self-review the full week's new code**
- *What:* read through `checks/raw_checks.yml`, `checks/run_quality_gate.py`, and the reconciled configuration from Day 19 as a whole, checking for consistency, dead code, and adherence to established house style
- *Expected output:* cleanup list, then fixes applied

**Task 4 — Merge and tag**
- *Steps:* merge `feature/data-quality-framework` into `main`, delete the branch, `git tag v0.5-phase4-complete && git push --tags`

**Task 5 — Write the milestone-level README update**
- *What:* mark Phase 4 complete; write a genuinely substantive "Data Quality Framework" section describing the three-layer check design and *why* each layer's checks differ in kind (Days 11, 17, 19's reasoning, condensed into clear prose); expand the architecture diagram to show the quality gates explicitly as part of the pipeline flow, not just an afterthought; update run instructions to include the single unified `run_quality_gate.py` command
- *Why this design was chosen:* a README that can explain *why* three different check suites look different, not just *that* they exist, is a genuinely stronger signal of real understanding than an equivalent README that just lists "implemented data quality checks" as a bullet point
- *Verify:* read the new section as a stranger would — does it teach something, or just describe something?

**Task 6 — Draft the first version of your "why this project" narrative**
- *What:* per Implementation Guide Section 12, write a short (3-5 sentence) README opening or `docs/design_decisions.md` summary explaining what makes this project's engineering genuinely interesting — the incremental MERGE design, the proven crash-safety, the three-layer quality framework tailored to what's actually risky at each stage — grounded specifically in what you actually built and tested, not generic claims
- *Verify:* every claim in this narrative should be traceable to a specific thing you actually did and can point to (a specific test, a specific design doc entry) — if a sentence feels like it could apply to any generic data pipeline, revise it to be more specific to *this* one

**Task 7 — Portfolio screenshots and diagram**
- *What:* screenshot the unified quality gate's aggregated report output; update the architecture diagram to its most current, complete state (Raw → Bronze [gated] → Silver [gated], via one unified quality command)

### 8. Definition of Done
- [ ] Full cold-start chain including unified quality gate succeeds
- [ ] Crash-recovery re-verified against fully current code
- [ ] Self-review completed
- [ ] `feature/data-quality-framework` merged, tagged `v0.5-phase4-complete`
- [ ] README updated with a substantive Data Quality Framework section and a specific, grounded project narrative
- [ ] Architecture diagram at its most current, accurate state
- [ ] Portfolio screenshots saved

### 9. Validation Steps
The now-familiar bar, applied at the largest scope yet: could a stranger, with only your README, understand not just how to run this project but *why* it's engineered the way it is — and would every specific claim they read hold up if they asked you to demonstrate it live?

### 10. Common Beginner Mistakes
- Writing a generic "why this project" narrative that could describe any data pipeline, rather than one grounded in this project's specific, tested decisions
- Treating today's README update as just another incremental addition rather than recognizing this as a genuine milestone worth a more substantial pass
- Letting the "why three different check suites" explanation collapse into just listing what each contains, without the reasoning for *why* they differ

### 11. Debugging Guide
No new debugging territory today — if the cold-start chain fails anywhere, the relevant debugging guide from that specific day (8 through 20) still applies; use this as an opportunity to notice which debugging guide you reach for fastest, which is itself a useful signal of which concepts have become genuinely internalized versus still needing a reference.

### 12. Mentor Notes
- "Three weeks in, you now have a complete, working, tested data pipeline with real production patterns — incremental processing, crash-safety, and a genuinely tiered quality framework — built and validated the same disciplined way, five times over (Days 7, 12, 18, 21, and today's even larger validation). This is worth pausing on. A huge number of portfolio projects never get this rigorous a foundation, because the unglamorous discipline of cold-start testing and honest documentation is exactly the part that's easiest to skip under time pressure. You haven't skipped it once."
- "The 'why this project' narrative task today is genuinely one of the most valuable things you'll write in this whole project. Notice how much easier it was to write *specifically* and *credibly* today, with three weeks of real, tested decisions behind you, than it would have been to write generically on Day 1. This is exactly why the Implementation Guide's portfolio-optimization advice comes at the point in the guide it does, not at the very beginning — the narrative should follow the real engineering, not precede it."

### 13. Industry Insights
- Engineers who can explain *why* their system is designed a specific way, with specific evidence, consistently perform better in technical interviews than engineers who can only describe *what* their system does — today's narrative-writing task is direct practice for exactly that distinction, and it's grounded in genuinely real work, which is what makes it credible rather than rehearsed-sounding.
- A three-week-old project with five tagged, cold-start-verified milestones and a documented crash-recovery test is a meaningfully stronger artifact, at this stage, than most tutorial-following portfolio projects ever become, even after completion — this is worth genuine confidence, not just encouragement.

### 14. Git Workflow
- **Branch:** `main` (post-merge)
- **Commit/merge:** `feature/data-quality-framework` → `main`
- **Tag:** `v0.5-phase4-complete`

### 15. README Updates
Completed as Tasks 5–6 above — substantive Data Quality Framework section, grounded project narrative, updated architecture diagram and run instructions.

### 16. Portfolio Updates
New screenshots, fully current architecture diagram, and — for the first time this project — a README narrative genuinely worth sharing as-is in a LinkedIn post or portfolio link, grounded entirely in real, tested engineering decisions from the last three weeks.

### 17. Interview Questions
- "Tell me the story of your project's data quality approach — not just what you built, but why each layer's checks are different."
- "What's the single engineering decision in this project you're proudest of, and why?"

### 18. Learning Checkpoint — Full Week 3 Review
- Explain, in order, the exact sequence from MERGE execution to watermark advancement, and why that order is crash-safe.
- Describe the crash-recovery test you performed: what was simulated, what state resulted, and what proved recovery was safe.
- Why do Raw, Bronze, and Silver each have differently-shaped quality checks rather than one repeated set?
- Walk through how your unified quality gate script correctly aggregates three independent results without masking an early failure.

### 19. End-of-Day Checklist
- [ ] All 7 tasks complete
- [ ] Cold-start and crash-recovery both verified against the fully current codebase
- [ ] Merged, tagged, README/diagram/portfolio all substantially updated

### 20. Tomorrow's Preview
Week 4 opens Phase 5: dbt-based data modeling, starting with staging models built on top of this now-complete, quality-gated Silver layer — the first step toward the sessionization logic, dimensional models, and business marts that will eventually power the project's dashboards.

---

## 📦 WEEK 3 REVIEW

### Engineering Milestone Achieved
The full ingestion → Bronze → Silver pipeline is now complete, incremental, crash-safe (proven, not just claimed), and gated end-to-end by a genuine, three-layer, purpose-differentiated data quality framework with a single, reusable, Airflow-ready entry point. This closes out the entire "Raw data to trustworthy, deduplicated Silver table" arc of the project — everything remaining builds analytics and orchestration on top of a foundation you've now stress-tested repeatedly.

### Skills Gained This Week
- Delta Lake `MERGE INTO`, implemented via both SQL-conceptual design and the Python `DeltaTable` API, with disciplined post-write verification before any dependent state changes
- Precise operation-ordering design for crash-safety, and — critically — actually proving it via deliberate failure injection rather than only reasoning about it abstractly
- Layer-appropriate data quality check design: recognizing that Raw, Bronze, and Silver each warrant genuinely different checks, not repeated ones
- Multi-process result aggregation without failure-masking, and the specific test that proves aggregation logic is correct independent of the checks it wraps
- A now well-practiced cold-start validation and milestone documentation discipline, applied for the fourth time at the largest scope yet
- Writing a grounded, evidence-based project narrative, only once real engineering existed to justify it

### Portfolio Progress
`main` now holds a complete, five-times-cold-start-verified pipeline (tags `v0.1` through `v0.5`), with a genuinely substantive README including a real Reliability section and a specific, evidence-grounded project narrative. The architecture diagram accurately reflects Raw → Bronze (Delta, quality-gated) → Silver (Delta, incremental MERGE, quality-gated), and portfolio screenshots document real Delta version history, real passing quality gates, and a real aggregated quality report.

### Readiness Checklist for Week 4
Before Week 4 begins, confirm:
1. A full cold-start test (ingestion → Bronze → Silver → unified quality gate) still passes cleanly.
2. The crash-recovery test still passes against the current, fully merged codebase.
3. You can explain, without notes, why each of Raw/Bronze/Silver's quality checks is shaped differently.
4. `main` is merged through Phase 4 and tagged `v0.5-phase4-complete`.
5. You're comfortable with `DeltaTable.forPath(...).merge(...)`, Soda Core custom SQL checks, and subprocess-based result aggregation — these are all assumed working knowledge starting Week 4's dbt modeling work.

**Confirm these are genuinely true and I'll generate Week 4 — dbt staging models and the sessionization intermediate layer, the start of Phase 5's data modeling work.**