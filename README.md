# solid-jobs-py

[![Build Status](https://github.com/nicolasva/solid-jobs-py/actions/workflows/ci.yml/badge.svg)](https://github.com/nicolasva/solid-jobs-py/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/solid-jobs-py.svg)](https://pypi.org/project/solid-jobs-py/)
[![Downloads](https://static.pepy.tech/badge/solid-jobs-py)](https://pepy.tech/project/solid-jobs-py)
[![Documentation](https://img.shields.io/badge/docs-README-blue.svg)](https://github.com/nicolasva/solid-jobs-py#readme)

Port Python de [solid-jobs](../solid-jobs) (Ruby) : jobs fiables sur Redis, exécutés en parallélisme CPU réel
par des **sous-interpréteurs Python** (PEP 684 / PEP 734, un GIL par interpréteur).

**Redis n'est pas recodé en Python.** Tout l'accès Redis passe par un processus compagnon Ruby
(*sidecar*) qui embarque la gemme [`solid-redis`](https://rubygems.org/gems/solid-redis).

```
[ Processus Python ]
   |-- Subinterpreter 1 (GIL isolé) --\
   |-- Subinterpreter 2 (GIL isolé) --+--> Socket Unix (0600) --> Sidecar Ruby (solid-redis) --> Redis
   |-- Subinterpreter N (GIL isolé) --/        JSON lines
```

## Prérequis

- Python >= 3.14 (`concurrent.interpreters`)
- Ruby + `gem install solid-redis` (>= 1.0.13) ; `bundle install` fonctionne aussi via le `Gemfile`
- Un serveur Redis

## Installation

```sh
pip install .          # embarque solid_redis_gateway.rb
```

## Utilisation

```python
# myapp/jobs.py  (doit être importable par son nom de module dans les workers, pas __main__)
from solid_jobs import Task

class RecalculateAccount(Task, channel="critical", max_failures=10):
    def execute_task(self, account_id):
        ...

    @classmethod
    def retry_delay(cls, failure_count, error, envelope):
        return 5.0          # secondes, "drop", "archive" ou None (backoff par défaut)
```

```python
from myapp.jobs import RecalculateAccount

RecalculateAccount.enqueue(42)
RecalculateAccount.enqueue_many([[1], [2], [3]])
RecalculateAccount.enqueue_after(30, 42)
RecalculateAccount.schedule_at(datetime_or_timestamp, 42)
RecalculateAccount.with_options(channel="low").enqueue(42)
```

Exécuter les workers :

```sh
REDIS_URL=redis://127.0.0.1:6379/0 solid-jobs -r myapp.jobs -c 4 --channel critical,3 --channel default -t 25
```

Options : `-r/--require`, `-C/--config` (TOML ou JSON), `-c/--concurrency`, `--channel nom[,poids]`,
`-t/--timeout`, `--redis-url`. Signaux : `SIGTERM`/`SIGINT` arrêt gracieux, `SIGTSTP` quiet, `SIGTTIN` dump des threads.

Variables d'environnement : `REDIS_URL`, `SOLID_JOBS_RUBY` (commande ruby, ex. `bundle exec ruby`),
`SOLID_JOBS_GATEWAY_SOCKET` (utiliser un sidecar déjà lancé).

Tests : `Task.execute(...)` exécute en ligne ; `with lab.testing("capture"):` capture au lieu d'écrire dans Redis.

## Garanties (identiques à la version Ruby)

Même clavier de clés Redis (`solid_jobs:*`) et mêmes scripts Lua : réservation fiable (liste *claimed* par worker),
ack/requeue protégés par un jeton de claim (fencing), récupération des claims de nœuds morts, retries avec backoff,
archive des échecs, tâches planifiées, barrière de publication, `IntegrityCheck`.

## Protocole du sidecar

Une requête JSON par ligne, une réponse par ligne :

```
{"command": "RPOP", "args": ["queue"]}      -> {"status": "OK", "data": ...}
{"pipeline": [["SET","a",1],["INCR","a"]]}  -> {"status": "OK", "data": [...]}
erreur                                      -> {"status": "ERROR", "error": {"class": "...", "message": "..."}}
```

Un thread et un client `SolidRedis::Client` par connexion. Socket créée en `0600` dans un répertoire `0700`.
Le sidecar s'arrête quand le parent meurt (EOF sur stdin).

## Écarts par rapport au RFC

- `require "solid_redis"` (et non `solid-redis`) ; `SolidRedis.execute` n'existe pas : le sidecar utilise l'API réelle
  `SolidRedis.config(url:).new_client` + `call_v` / `pipelined`.
- Gateway multi-thread (une connexion = un thread) au lieu d'une boucle `accept` série, sinon un worker bloquerait les autres.
- Réservation via scripts Lua (`EVALSHA`) et non un simple `RPOP`.

## Volontairement non porté

Adaptateurs Rails/ActiveJob, warm pool, code spécifique Ractor, télémétrie SolidTrace (`observations.rb`).
Les hooks d'instrumentation des jobs sont conservés (l'instrumenteur doit être une classe sans argument).

## Benchmark : Celery vs solid-jobs-py

Environnement : Python 3.14.7, 12 CPU logiques (Apple Silicon), Celery 5.6.3, solid-jobs-py 0.1.0 + sidecar Ruby `solid-redis`,
Redis local isolé. Méthodologie identique au benchmark Sidekiq/SolidJobs (Ruby) : worker neuf par mesure, ordre des systèmes
alterné, **médiane de 6 répétitions de 1 000 jobs**, débit mesuré du premier au dernier job d'une file pré-remplie,
RSS/CPU de tout l'arbre de processus (sidecar Ruby inclus). Job CPU = boucle entière de 100 000 itérations, job IO = sleep 5 ms,
`mixed` = alternance des deux. Celery : pool `prefork`, `--prefetch-multiplier=1 -Ofair`, broker Redis, résultats ignorés.

| Workload | Conc. | System | Jobs/s | vs Celery | Queue p50 (ms) | Exec p50 (ms) | CPU-s/1,000 jobs | Peak RSS (MiB) | Marginal RSS/unit (MiB) | jobs/s/MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| cpu | 1 | celery | 134 | 1.00x | 4018 | 7.3 | 7.69 | 84 | - | 1.60 |
| cpu | 1 | solid-jobs-py | 134 | 1.00x | 4091 | 7.1 | 7.21 | 67 | - | 2.01 |
| cpu | 2 | celery | 261 | 1.00x | 2236 | 7.4 | 7.83 | 110 | 26.5 | 2.37 |
| cpu | 2 | solid-jobs-py | 263 | 1.01x | 2261 | 7.2 | 7.34 | 77 | 10.6 | 3.40 |
| cpu | 4 | celery | 520 | 1.00x | 1325 | 7.3 | 7.77 | 164 | 26.7 | 3.17 |
| cpu | 4 | solid-jobs-py | 501 | 0.96x | 1382 | 7.4 | 7.64 | 99 | 10.7 | 5.08 |
| cpu | 8 | celery | 957 | 1.00x | 917 | 7.5 | 8.46 | 270 | 26.4 | 3.55 |
| cpu | 8 | solid-jobs-py | 924 | 0.97x | 933 | 7.7 | 8.21 | 141 | 10.7 | 6.54 |
| io | 1 | celery | 166 | 1.00x | 3298 | 5.8 | 0.66 | 84 | - | 1.98 |
| io | 1 | solid-jobs-py | 147 | 0.88x | 3741 | 6.4 | 0.34 | 67 | - | 2.20 |
| io | 2 | celery | 330 | 1.00x | 1823 | 5.8 | 0.62 | 111 | 26.5 | 2.98 |
| io | 2 | solid-jobs-py | 295 | 0.89x | 2054 | 6.4 | 0.34 | 78 | 10.9 | 3.80 |
| io | 4 | celery | 654 | 1.00x | 1101 | 5.8 | 0.54 | 164 | 26.7 | 3.98 |
| io | 4 | solid-jobs-py | 583 | 0.89x | 1231 | 6.4 | 0.38 | 99 | 10.6 | 5.91 |
| io | 8 | celery | 1311 | 1.00x | 778 | 5.7 | 0.52 | 270 | 26.5 | 4.85 |
| io | 8 | solid-jobs-py | 1114 | 0.85x | 837 | 6.4 | 0.45 | 141 | 10.7 | 7.88 |
| mixed | 1 | celery | 149 | 1.00x | 3660 | 7.3 | 4.13 | 84 | - | 1.77 |
| mixed | 1 | solid-jobs-py | 138 | 0.93x | 3963 | 7.4 | 3.88 | 67 | - | 2.06 |
| mixed | 2 | celery | 294 | 1.00x | 2024 | 7.3 | 4.16 | 111 | 26.5 | 2.66 |
| mixed | 2 | solid-jobs-py | 280 | 0.95x | 2147 | 7.1 | 3.77 | 78 | 10.7 | 3.61 |
| mixed | 4 | celery | 582 | 1.00x | 1211 | 7.3 | 4.15 | 164 | 26.7 | 3.55 |
| mixed | 4 | solid-jobs-py | 553 | 0.95x | 1279 | 7.2 | 3.84 | 99 | 10.7 | 5.59 |
| mixed | 8 | celery | 1133 | 1.00x | 838 | 7.4 | 4.20 | 271 | 26.7 | 4.18 |
| mixed | 8 | solid-jobs-py | 1059 | 0.93x | 863 | 7.4 | 4.03 | 141 | 10.5 | 7.52 |

Rapport complet (enqueue, p95/p99, CPU %, efficacité de scaling, CSV) et code :
`../benchmark_sidekiq_solid-jobs/celery_vs_solid_jobs_py/` (`python bench.py`).

**Lecture des résultats (honnête) :**

- **Débit : parité en CPU, Celery devant de 5 à 15 % en IO/mixed, mais ce n'est pas un coût de solid-jobs-py.**
  Celery fork déjà ses workers, donc les sous-interpréteurs n'ajoutent pas de débit CPU. L'écart IO vient de `time.sleep(5 ms)`
  (la « charge IO » du benchmark) : sous macOS il dure ~5,6 ms dans un enfant forké (Celery prefork) contre ~6,2 ms dans le
  processus principal, un thread ou un sous-interpréteur (mesuré hors de tout framework). C'est l'écart de ~0,6 ms/job observé.
- **Surcoût du framework : plus faible que Celery.** Avec des jobs vides (`noop`, 5 000 jobs, médiane de 4 runs), après
  l'optimisation « ack + réservation en un aller-retour » (ci-dessous) : 1 worker 4 481 vs 2 309 jobs/s (1,94x),
  4 workers 7 640 vs 2 494 jobs/s (3,06x).
- **Optimisations testées** (avant → après, même machine, médianes) :
  - *Ack fusionné avec la réservation suivante* (implémenté) : l'ack part dans le même pipeline que la prochaine réservation,
    soit 1 aller-retour Redis par job au lieu de 2, avec le même fencing. `noop` : 3 697 → 4 481 jobs/s (+21 %) à c=1,
    5 963 → 7 640 jobs/s (+28 %) à c=4. En IO/mixed le gain (~50 µs sur ~6 ms) est sous le bruit de mesure :
    IO c=8 1 114 → 1 133, mixed c=8 1 059 → 1 095 jobs/s ; Celery reste devant en IO (0,86–0,91x).
  - *Script Lua unique* : déjà en place (réservation + jeton de fencing + claim en un seul script).
  - *MessagePack* : **rejeté**. Dans un sous-interpréteur, `msgpack` retombe sur son implémentation Python pure
    (extension C non utilisée) : ~26 µs par aller-retour contre ~6 µs pour le `json` C de la bibliothèque standard.
- **Mémoire : net avantage solid-jobs-py.** À concurrence 8 : ~141 MiB contre ~270 MiB (environ -48 %), soit ~10,7 MiB par
  worker supplémentaire contre ~26,5 MiB pour un process forké. Résultat : 1,1 à 1,8x plus de jobs/s par MiB.
  Cela confirme l'économie mémoire (~40 %) annoncée dans le RFC.
- **Fiabilité :** 144 mesures, toutes à 1 000/1 000 jobs exécutés : zéro erreur, zéro doublon. Ces garanties (réservation avec jetons
  de claim / fencing, récupération des nœuds morts) ne sont pas mesurées ici ; seul le débit, la latence et la mémoire le sont.
- Aucune latence d'exécution n'est dégradée : p50 d'exécution identique (~7 ms CPU), ~0,6 ms de plus en IO.
- Benchmark local synthétique, non une prédiction de capacité applicative. Celery tourne ici avec le start method `fork`
  forcé (macOS utilise `spawn` par défaut, non supporté par son pool prefork) et `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES`.

Ce benchmark a aussi mis en évidence deux défauts corrigés dans solid-jobs-py : `interpreters.Queue.get(timeout=…)` fait de
l'attente active en Python 3.14 (remplacé par `get_nowait()` + sommeil) et une course au démarrage où la récupération pouvait remettre
en file les claims du nœud avant son premier heartbeat (désormais publié avant le démarrage des workers).

### Stress (concurrence 4 / 8 / 16)

5000 jobs, 3 répétitions, médianes (`results/stress-c4-8-16.md`). Débit en jobs/s, mémoire = pic RSS de l'arbre de processus.

| Charge | c | Celery | solid-jobs-py | Ratio | RSS Celery | RSS solid-jobs-py |
|---|---:|---:|---:|---:|---:|---:|
| CPU | 4 | 526 | 502 | 0,95x | 165 MiB | 99 MiB |
| CPU | 8 | 929 | 919 | 0,99x | 273 MiB | 142 MiB |
| CPU | 16 | 946 | 1023 | 1,08x | 467 MiB | 228 MiB |
| IO | 4 | 664 | 590 | 0,89x | 164 MiB | 99 MiB |
| IO | 8 | 1336 | 1154 | 0,86x | 272 MiB | 142 MiB |
| IO | 16 | 2170 | 2153 | 0,99x | 483 MiB | 226 MiB |
| Mixed | 4 | 589 | 560 | 0,95x | 165 MiB | 100 MiB |
| Mixed | 8 | 1163 | 1097 | 0,94x | 272 MiB | 143 MiB |
| Mixed | 16 | 1796 | 1877 | 1,04x | 487 MiB | 227 MiB |

Celery garde l'avantage en IO à faible concurrence (artefact du `sleep` en sous-interpréteur, voir plus haut) ; à c=16
solid-jobs-py rattrape ou dépasse Celery en CPU et en mixed, avec environ moitié moins de mémoire. Aucun job perdu.

### Stabilité (5 minutes par système)

4 workers, jobs CPU/IO mélangés, file maintenue à ~1000, un échantillon toutes les 10 s (`stability.py`, durée via
`BENCHMARK_STABILITY_DURATION`, 1800 s par défaut comme la version Ruby). Les résultats ci-dessous sont des runs courts
de 300 s, pas de 30 min.

| Système | Jobs/s médian | Jobs/s min | RSS début → fin | Pente RSS | Jobs terminés |
|---|---:|---:|---:|---:|---:|
| Celery | 590 | 569 | 165 → 165 MiB | +1,3 MiB/h | 176 734 |
| solid-jobs-py | 556 | 536 | 100 → 100 MiB | +3,5 MiB/h | 166 659 |

Débit et mémoire sont plats sur les deux systèmes : pas de fuite visible sur 5 min (une conclusion sur la durée
demande le run complet de 30 min). Débit mixed à c=4 : Celery ~6 % devant.

Commandes : `BENCHMARK_CONCURRENCY=4,8,16 BENCHMARK_REPETITIONS=3 BENCHMARK_JOBS=5000 BENCHMARK_NAME=stress-c4-8-16 .venv/bin/python bench.py`
puis `BENCHMARK_STABILITY_DURATION=300 .venv/bin/python stability.py`.

### Récapitulatif

| Axe | Résultat (solid-jobs-py vs Celery) |
|---|---|
| Débit CPU | Parité (0,95x à 1,08x selon la concurrence) |
| Débit mixed | 0,93–0,96x à c ≤ 8, 1,04x à c=16 |
| Débit IO | Celery devant de 5 à 14 % à c ≤ 8 (artefact du `sleep` en sous-interpréteur), parité à c=16 |
| Surcoût du framework (`noop`) | 1,94x (c=1) à 3,06x (c=4) plus rapide |
| Mémoire | environ −48 % (141 vs 270 MiB à c=8), 10,7 vs 26,5 MiB par worker |
| Stabilité (5 min) | Débit et RSS plats pour les deux, aucun job perdu ; run de 30 min non fait |
| Optimisations | Ack fusionné avec la réservation : fait (+21 à +28 % en `noop`). Lua unique : déjà en place. MessagePack : rejeté (plus lent en sous-interpréteur) |

En résumé : solid-jobs-py égale Celery en CPU et mixed, le dépasse nettement en surcoût et en mémoire, et reste derrière
de quelques pourcents en IO à faible concurrence.

## Développement

```sh
redis-server --port 6390 --save "" --appendonly no &
REDIS_URL=redis://127.0.0.1:6390/15 PYTHONPATH=src python3 -m unittest discover -s tests -t .
```

Les tests utilisent `FLUSHDB` sur la base indiquée : n'utilisez pas une base de production.
