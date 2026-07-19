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
