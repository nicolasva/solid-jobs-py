# Changelog

## Unreleased

- Performance : l'ack d'un job est envoyé dans le même pipeline Redis que la réservation suivante (1 aller-retour par job au lieu de 2, fencing inchangé).

## 0.1.0

- Port Python de solid-jobs : workers en sous-interpréteurs (PEP 734), Redis via un sidecar Ruby embarquant `solid-redis`.
- Reserve/ack/requeue fiables avec fencing, recovery, retries, planification, `IntegrityCheck`, CLI `solid-jobs`.
