FROM apache/nifi:1.25.0

USER root

# ── Dépendances système ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-pip \
        ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Pipeline scripts ──────────────────────────────────────────────────────────
RUN mkdir -p /opt/physical-data-pipeline/bin \
             /opt/physical-data-pipeline/logs \
             /opt/physical-data-pipeline/run

COPY bin/ /opt/physical-data-pipeline/bin/
COPY requirements.txt /opt/physical-data-pipeline/requirements.txt

# ── Venv Python isolé ─────────────────────────────────────────────────────────
RUN python3 -m venv /opt/physical-data-pipeline/venv \
    && /opt/physical-data-pipeline/venv/bin/pip install --upgrade pip --quiet \
    && /opt/physical-data-pipeline/venv/bin/pip install \
        -r /opt/physical-data-pipeline/requirements.txt --quiet

# ── Permissions ───────────────────────────────────────────────────────────────
RUN chmod +x /opt/physical-data-pipeline/bin/*.py \
    && chown -R nifi:nifi /opt/physical-data-pipeline

# ── Dossiers NAS (points de montage) ─────────────────────────────────────────
RUN mkdir -p \
        /nas/physical_data/inbox \
        /nas/physical_data/work \
        /nas/physical_data/processed \
        /nas/physical_data/quarantine \
        /nas/physical_data/archive \
        /nas/physical_data/manifests \
    && chown -R nifi:nifi /nas

USER nifi
