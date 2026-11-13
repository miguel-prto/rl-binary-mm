# CLAUDE.md — TFG: Adaptive Liquidity Provision in Prediction Markets Using RL

## Contexto
- TFG de Ingeniería Informática (doble grado Matemáticas e Informática, UCM). Departamento de IA, 12 ECTS, entrega en junio de 2027.
- Autor: Miguel Prieto. La memoria es en español; el código, comentarios y README, en inglés.
- El plan completo está en `PLAN_TFG.md`. Léelo antes de proponer cambios de alcance.
- El repositorio será público y es pieza de portfolio (objetivo principal): prioriza claridad, tests y reproducibilidad.

## Resumen de decisiones
- Mercados binarios cripto "Up/Down" de 15 min. BTC es el principal; ETH y SOL sirven para generalización.
- Plataformas:
  - Polymarket, principal para el entorno. Resuelve con el TWAP de Chainlink.
  - Kalshi KXBTC15M, segunda plataforma. Resuelve con CF Benchmarks BRTI (media del último minuto).
- Simulador: núcleo en C++20 (order book L3 con IDs de orden) con bindings en Python. El resto, en Python.
- RL: Gymnasium + Stable-Baselines3 (PPO y SAC), entrenamiento en CPU.
- Datos: Kalshi y Polymarket dan L2 (más fills on-chain a nivel de orden en Polymarket), no L3. El simulador es L3 internamente y reconstruye eventos a partir de los deltas L2 (ver PLAN_TFG.md §3.0).
- El recorder está en un repo aparte, `market-recorder` (paquete `marketrec`, en `../market-recorder`), compartido con el TFG de Matemáticas. Aquí solo se lee el raw (`marketrec.raw.iter_raw_records`) y se normaliza; los cambios de grabación se hacen allí.
- Grabación más amplia que el alcance del TFG: todas las duraciones cripto "Up/Down" (BTC, ETH, SOL, XRP) y una selección de mercados macro/políticos, por si se amplía. Cripto 15 min siempre aislado del resto (ver PLAN_TFG.md §2 y `docs/design.md` en market-recorder).
- Sin dinero real. Como mucho, "shadow trading" (feed real, fills simulados de forma conservadora).
- Fuera de alcance por ahora: multi-resultado y multiagente.

## Cómo quiero trabajar contigo
- Quiero entender todo el código. Explica las decisiones de diseño, ve por pasos pequeños y revisables, y no generes módulos enormes de golpe.
- Antes de implementar algo no trivial, propón el diseño (interfaces y estructuras de datos) y espera mi visto bueno.
- Cada módulo nuevo lleva tests (GoogleTest en C++, pytest en Python).
- Si un dato de una API (endpoint, formato, comisión) no está verificado, dilo y compruébalo en la documentación oficial antes de codificarlo.
- Nada de credenciales en el repo: usa variables de entorno o `.env` (incluido en `.gitignore`).

## Estructura prevista
```
cpp/            núcleo C++ (include/, src/, tests/, bench/, bindings/)
python/rl_binary_mm/  data/ store/ calibration/ market/ env/ agents/ eval/ live/
experiments/    configs + scripts reproducibles
notebooks/      EDA y figuras
memoria/        LaTeX
```

## Convenciones
- C++: C++20, CMake, clang-format, sin dependencias pesadas. Los precios van en ticks enteros (nada de float en el matching).
- Python 3.12: ruff, type hints y Polars + DuckDB + Parquet para los datos.
- Todos los timestamps en UTC, en nanosegundos o milisegundos enteros y documentados por campo.
- Experimentos reproducibles: config versionada, semilla fija y registro en MLflow o W&B.

## Fase actual
Fase 1 (nov–dic 2026): recorder de libros y trades (Polymarket, Kalshi y spot BTC) desplegable en un VPS, esquema normalizado en Parquet y descarga de histórico de trades.
