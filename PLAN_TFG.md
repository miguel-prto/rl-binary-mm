# Plan del TFG — Provisión adaptativa de liquidez en mercados de predicción mediante aprendizaje por refuerzo

**Título (en):** Adaptive Liquidity Provision in Prediction Markets Using Reinforcement Learning
**Autor:** Miguel Prieto · Doble grado Matemáticas e Informática (UCM)
**TFG de Informática** · Departamento de IA · 12 ECTS (~300–360 h) · Entrega: junio 2027
**Memoria:** en español · **Código:** en inglés (identificadores, comentarios, README)

---

## 1. Decisiones tomadas

| Tema | Decisión |
|---|---|
| Tipo de mercado | Binarios cripto "Up/Down" de 15 minutos (BTC principal; ETH/SOL para generalización) |
| Plataformas | Polymarket (principal para el entorno RL) + Kalshi (segunda plataforma: generalización y capítulo de discrepancias entre plataformas) |
| Resultados | Solo binario. Multi-resultado y multiagente → trabajo futuro |
| Simulador | Núcleo en C++20 (motor LOB con IDs de orden, nivel L3) + bindings Python (nanobind/pybind11). En la memoria, un capítulo compacto: es la herramienta, no el foco |
| Foco | RL (departamento de IA): parametrización de acciones, diseño de recompensa, robustez sim→real, evaluación rigurosa |
| Live | "Shadow trading": el agente cotiza contra el feed real con fills simulados de forma conservadora. Sin dinero real. Opcional si hay tiempo |
| Portfolio | Objetivo principal → repo público, reproducible, CI, README cuidado; posible preprint en arXiv |
| Presupuesto | Bajo: VPS para el recorder (~5–10 €/mes). Entrenamiento en CPU. Total estimado < 100 € |

### Por qué los mercados de 15 minutos
- **Muchos episodios:** 96 ventanas al día por activo y plataforma, que se resuelven solas. Hay miles de episodios independientes en pocas semanas.
- **Valor justo observable:** la probabilidad "justa" de Up se puede aproximar desde el precio spot del subyacente: p* ≈ Φ( ln(S_t/S_0) / (σ√τ) ), una opción digital. Esto permite medir la selección adversa y construir un simulador con fundamento.
- **Selección adversa real y medible:** hay arbitrajistas de latencia que reaccionan a movimientos del spot.
- **Comisiones e incentivos relevantes:** taker fees en forma p(1−p), maker rebates en Polymarket y fills maker sin comisión en Kalshi KXBTC15M.

### Diferencia clave entre plataformas (afecta al arbitraje)
- **Kalshi (KXBTC15M):** se liquida con el índice CF Benchmarks BRTI, promediado en el último minuto de la ventana.
- **Polymarket:** se liquida con el TWAP de Chainlink Data Streams (BTC/USD, 60 s en los de 15 min).

→ Un aparente arbitraje entre plataformas conlleva **riesgo de base por la fuente de resolución**. Esto es un resultado analizable en sí mismo, no solo un obstáculo.

---

## 2. Pregunta de investigación, objetivos e hipótesis

**Pregunta.** En mercados binarios de corta duración, ¿puede un agente de RL proveer liquidez con mejor rentabilidad ajustada al riesgo que las estrategias analíticas (spread fijo, Avellaneda–Stoikov, AS en logit, Feil–Nendel), y adaptarse a la selección adversa, la cercanía a la resolución y los cambios de régimen de volatilidad?

**Objetivos**
- **O1. Infraestructura de datos.** Recorder 24/7 de libros (WebSocket) en ambas plataformas, más spot del subyacente, más histórico de trades (Kalshi API; Polymarket on-chain/Data API). Almacenamiento en Parquet y DuckDB.
- **O2. Simulador.** Motor LOB en C++ (prioridad precio-tiempo, colas, latencia, comisiones y rebates, YES/NO), más un generador de flujo calibrado con O1.
- **O3. Entorno.** Gymnasium con modos sintético, replay de libros grabados y shadow (live).
- **O4. Agentes.** Baselines analíticos; PPO y SAC (Stable-Baselines3). Dos parametrizaciones de acción: directa e híbrida sobre un quoter analítico.
- **O5. Evaluación.** Múltiples semillas, intervalos de confianza, descomposición del PnL, markouts, pruebas de estrés, generalización entre activos y plataformas.
- **O6 (opcional).** Shadow trading y análisis de la brecha sim→real. Análisis de discrepancias entre Kalshi y Polymarket.

**Alcance de la grabación (decisión de 12-nov-2026).** El recorder graba más de lo que el TFG necesita hoy, porque los libros no se pueden descargar a posteriori:
- **Cripto:** todas las duraciones "Up/Down" de Polymarket (5 min, 15 min, 1 h, 4 h) para BTC, ETH, SOL y XRP, y las series de 15 min de Kalshi para los mismos activos.
- **Macro y política (posible ampliación):** libros por WebSocket solo de una lista seleccionada (Fed, IPC, empleo, elecciones…) filtrada por volumen y con tope de mercados; además, un catálogo horario por REST (metadatos y mejores precios) de todos los mercados activos de ambas plataformas.
- Los dos ámbitos corren en contenedores separados: un fallo en macro nunca afecta a los datos de cripto de 15 min, que son el núcleo del TFG.

**Hipótesis**
- **H1.** La parametrización híbrida (RL que ajusta γ, spread y skew de un quoter AS en logit) aprende de forma más estable y generaliza mejor que la acción directa.
- **H2.** Con el spot del subyacente en la observación, el agente reduce la pérdida por selección adversa (markouts) frente a los baselines.
- **H3.** La aleatorización de dominio de los parámetros del simulador reduce la degradación al pasar de sintético a replay.
- **H4.** El agente aprende a ensanchar el spread o retirarse cerca de la resolución, y reduce el CVaR sin perder la mayor parte del PnL por spread.

---

## 3. Arquitectura

```
repo/
├── cpp/                 # núcleo C++20: order book L3, matching, colas, latencia, fees
│   ├── include/ src/ tests/ (GoogleTest) bench/ (Google Benchmark)
│   └── bindings/        # nanobind/pybind11 → módulo Python `pmsim`
├── python/tfg/
│   ├── data/            # clientes REST/WS Kalshi y Polymarket, spot (Coinbase/Binance), on-chain
│   ├── store/           # esquema normalizado, escritura Parquet, consultas DuckDB
│   ├── calibration/     # volatilidad del subyacente, intensidades de llegada λ(δ)=A·e^{-kδ}, Hawkes, tamaños
│   ├── market/          # modelo generativo: subyacente, valor justo, traders de fondo
│   ├── env/             # Gymnasium: obs/acción/recompensa; modos synthetic | replay | shadow
│   ├── agents/          # baselines + wrappers RL (SB3)
│   ├── eval/            # métricas, PnL attribution, markouts, tests estadísticos, informes
│   └── live/            # shadow trading
├── recorder/            # servicio desplegable (Docker) para el VPS
├── experiments/         # configs (Hydra o YAML) + scripts reproducibles
├── notebooks/           # EDA y figuras de la memoria
├── memoria/             # LaTeX de la memoria
└── .github/workflows/   # CI: build C++, tests, lint
```

**Stack.**
- C++20, CMake, GoogleTest, nanobind.
- Python 3.12, NumPy, Polars, DuckDB, PyArrow, Gymnasium, Stable-Baselines3, PyTorch (CPU), Optuna, Hydra.
- MLflow o W&B (plan gratuito), pytest, ruff, Docker.

### 3.0 Nivel de los datos disponibles
- **Ninguna de las dos plataformas publica L3.**
  - Kalshi: libro L2 agregado por precio (snapshot y deltas por WebSocket); trades sin IDs de orden.
  - Polymarket: libro L2 (snapshot y `price_change`). Los fills on-chain (`OrderFilled`) sí identifican cada orden ejecutada; altas y cancelaciones no.
- **El simulador es L3 por dentro**, porque necesita seguir la posición en la cola de las órdenes del agente. El flujo de fondo se genera de forma sintética, o se infiere de los deltas L2 del replay:
  - un aumento de tamaño en un nivel se trata como alta;
  - una disminución con trade, como ejecución;
  - una disminución sin trade, como cancelación.
- **Posición en la cola en replay:** el agente entra al final de la cola. Las cancelaciones se reparten pro-rata (supuesto central) y se compara con el peor caso (todas por detrás del agente) como cota pesimista.

### 3.1 Simulador (resumen técnico)
- **Motor (C++).** Eventos limit/market/cancel con ID de orden; prioridad precio-tiempo; posición en la cola de las órdenes del agente; tick configurable (0,01 por defecto); comisiones parametrizables por plataforma (Kalshi: 0,07·C·P·(1−P) taker; Polymarket: taker fee por categoría más maker rebate); latencia de envío y cancelación; liquidación a 0/1.
- **Modelo de mercado (Python, llamando al motor).**
  - Subyacente S_t: GBM con volatilidad estocástica o por régimen más saltos, calibrado con spot de 1 s.
  - Valor justo p*_t: precio de la digital (incluir la aproximación del TWAP final como mejora).
  - Takers informados: observan S antes que el resto (ventaja de latencia) y operan cuando |p* − precio| supera su umbral.
  - Takers ruidosos: Poisson o Hawkes, con tamaños de la distribución empírica.
  - Makers competidores: cotizan alrededor de p* con ruido y spread aleatorio.
- **Validación.** Distribución de spreads, profundidad por niveles, autocorrelación del signo de las órdenes, perfil de volatilidad frente a τ y p, y fill rate frente a distancia al mid: sintético contra los datos grabados.

### 3.2 MDP
- **Observación:**
  - q/q_max y τ/T;
  - mid y logit(mid); p* y (p* − mid) (señal del subyacente); σ̂ del subyacente;
  - spread, desequilibrio del libro en N niveles e intensidad y desequilibrio del flujo reciente;
  - posición en la cola de las órdenes propias y PnL no realizado.
- **Acción A (directa):** (δ_bid, δ_ask) en ticks más tamaño, discreta o continua, con opción de no cotizar un lado.
- **Acción B (híbrida):** (γ, multiplicador de spread, skew adicional) que alimentan un quoter AS en logit.
- **Recompensa:** r_t = ΔPnL_MtM − η·q_t²·σ²_p(τ) + rebates − comisiones. En t = T se liquida a 0/1. Ablación: PnL puro, penalización asimétrica (dampened) y penalización CVaR.
- **Episodio:** una ventana de 15 min. Paso de decisión de 1 s (configurable).

### 3.3 Baselines
1. Spread fijo simétrico.
2. Avellaneda–Stoikov "ingenuo" en espacio de precio.
3. AS en logit (con σ dependiente de τ).
4. Feil–Nendel (2026): control óptimo para binarios con riesgo de liquidación.
5. Quoter con valor justo: centra en p* con spread fijo (baseline fuerte e informado).

### 3.4 Evaluación
- **Métricas:** PnL, Sharpe por episodio, máximo drawdown, CVaR 5%, inventario medio y máximo, fill rate, % de tiempo cotizando.
- **Atribución del PnL:** spread capturado, inventario, selección adversa (markouts a +1 s, +10 s, +60 s), comisiones y rebates, liquidación.
- **Protocolo:**
  - división temporal train/val/test (sin fuga de datos) y 10 o más semillas;
  - IQM e intervalos bootstrap (rliable);
  - estrés: más informados, más latencia, saltos más grandes, comisiones distintas;
  - generalización a ETH/SOL y a la otra plataforma;
  - curva de PnL frente a latencia del agente (responde a "¿es viable en vivo?").

---

## 4. Planificación (12 ECTS ≈ 300–360 h)

| Periodo | Entregable | Mínimo viable |
|---|---|---|
| **nov–dic 2026** | Recorder desplegado (Polymarket, Kalshi y spot). Descarga de histórico de trades. Bibliografía. Anteproyecto al tutor | ✔ |
| **ene 2027** | Motor LOB en C++ con tests y bindings. Generador sintético simple. Baselines 1–2 | ✔ |
| **feb 2027** | EDA y calibración con datos grabados. Baselines 3–5. Validación de hechos estilizados | ✔ |
| **mar 2027** | Entorno Gymnasium (synthetic y replay). PPO con acción B. Diseño de la recompensa | ✔ |
| **abr 2027** | Experimentos: acción A frente a B, ablaciones, aleatorización de dominio, estrés, generalización | ✔ (parcial) |
| **may 2027** | Shadow trading (opcional). Capítulo entre plataformas (opcional). Redacción intensiva | — |
| **jun 2027** | Revisión, depósito, defensa | ✔ |

**Mínimo viable si hay retrasos:** recorder, motor C++, simulador sintético, 3 baselines, PPO híbrido y evaluación en sintético más replay. Todo lo demás es ampliación.

### Riesgos
| Riesgo | Mitigación |
|---|---|
| El C++ consume demasiado tiempo | Alcance mínimo del motor (sin tipos de orden exóticos); prototipo Python como referencia para los tests |
| El RL no supera al quoter con valor justo | La contribución es el análisis (cuándo y por qué). Con la acción híbrida, el RL nunca queda muy por debajo de su base |
| Pocos datos L2 | Arrancar el recorder ya. Respaldo: proveedores externos (p. ej. Oddpool para Kalshi; Marketlens para libros L2 de Polymarket desde mar 2026, de pago y sin redistribución, solo como complemento validado contra nuestros datos) |
| Cambios en APIs o comisiones | Capa de adaptadores; comisiones como configuración |

---

## 5. Índice tentativo de la memoria

1. Introducción (motivación, objetivos, contribuciones, estructura)
2. Mercados de predicción: CLOB frente a AMM (LMSR), Kalshi y Polymarket, mercados de 15 min, oráculos de resolución, comisiones e incentivos
3. Market making: Ho–Stoll, Glosten–Milgrom, Kyle; Avellaneda–Stoikov y GLFT; el caso binario (logit, Feil–Nendel)
4. Aprendizaje por refuerzo: MDP, gradiente de política, PPO, SAC; aleatorización de dominio
5. Estado del arte: RL para market making; market making en mercados de predicción
6. Datos: infraestructura, EDA, hechos estilizados *(fuerte en informática)*
7. Simulador y entorno *(compacto: arquitectura, decisiones, validación, rendimiento)*
8. Agentes y entrenamiento
9. Experimentos y resultados
10. Conclusiones, limitaciones, trabajo futuro (multiagente, multi-resultado, arbitraje entre plataformas, ejecución real)

Anexos: requisitos y diseño resumidos, guía de reproducción, configuración de experimentos.

---

## 6. Bibliografía base

**Microestructura y market making**
- Ho, T., Stoll, H. (1981). Optimal dealer pricing under transactions and return uncertainty. *J. Financial Economics*.
- Glosten, L., Milgrom, P. (1985). Bid, ask and transaction prices in a specialist market with heterogeneously informed traders. *J. Financial Economics*.
- Kyle, A. (1985). Continuous auctions and insider trading. *Econometrica*.
- Avellaneda, M., Stoikov, S. (2008). High-frequency trading in a limit order book. *Quantitative Finance*.
- Guéant, O., Lehalle, C.-A., Fernandez-Tapia, J. (2013). Dealing with the inventory risk: a solution to the market making problem. *Mathematics and Financial Economics*.
- Guéant, O. (2016). *The Financial Mathematics of Market Liquidity*. CRC Press.
- Cartea, Á., Jaimungal, S., Penalva, J. (2015). *Algorithmic and High-Frequency Trading*. Cambridge University Press.
- Cont, R., Stoikov, S., Talreja, R. (2010). A stochastic model for order book dynamics. *Operations Research*.
- Gould, M. et al. (2013). Limit order books. *Quantitative Finance*.
- Bouchaud, J.-P., Bonart, J., Donier, J., Gould, M. (2018). *Trades, Quotes and Prices*. Cambridge University Press.
- Bacry, E., Mastromatteo, I., Muzy, J.-F. (2015). Hawkes processes in finance. *Market Microstructure and Liquidity*.

**Mercados de predicción**
- Wolfers, J., Zitzewitz, E. (2004). Prediction markets. *J. Economic Perspectives*.
- Manski, C. (2006). Interpreting the predictions of prediction markets. *Economics Letters*.
- Hanson, R. (2003). Combinatorial information market design. *Information Systems Frontiers*.
- Chen, Y., Pennock, D. (2007). A utility framework for bounded-loss market makers. *UAI*.
- Othman, A., Pennock, D., Reeves, D., Sandholm, T. (2013). A practical liquidity-sensitive automated market maker. *ACM TEAC*.
- Feil, D., Nendel, M. (2026). Optimal Market Making in Prediction Markets. arXiv:2607.17991.
- Dalen, S. (2025). Toward Black–Scholes for Prediction Markets. arXiv:2510.15205.
- Xi, W., Moallemi, C., Pai, M., Wang, S. (2026). Volatility in Prediction Markets: A Structural Approach. arXiv:2607.08199.
- Jain, N. (2026). Optimal Market Making in Prediction Markets: Empirical Distribution Learning, Analytical Policy Characterization, and RL. TFG, Harvard.
- (2026). Prediction market spread modelling and analysis. Tesis de máster, Universidad de Tartu.

**RL y RL para market making**
- Sutton, R., Barto, A. (2018). *Reinforcement Learning: An Introduction* (2.ª ed.).
- Schulman, J. et al. (2017). Proximal Policy Optimization Algorithms. arXiv:1707.06347.
- Haarnoja, T. et al. (2018). Soft Actor-Critic. *ICML*.
- Tobin, J. et al. (2017). Domain randomization for transferring deep neural networks from simulation to the real world. *IROS*.
- Raffin, A. et al. (2021). Stable-Baselines3. *JMLR*.
- Towers, M. et al. (2024). Gymnasium. arXiv:2407.17032.
- Spooner, T., Fearnley, J., Savani, R., Koukorinis, A. (2018). Market Making via Reinforcement Learning. *AAMAS*.
- Spooner, T., Savani, R. (2020). Robust Market Making via Adversarial Reinforcement Learning. *IJCAI*.
- Gašperov, B. et al. (2021). Reinforcement learning approaches to optimal market making. *Mathematics*.
- Falces Marín, J. et al. (2022). A reinforcement learning approach to improve the performance of the Avellaneda-Stoikov market-making algorithm. *PLOS ONE*.
- Jerome, J., Palmer, G., Savani, R. (2022). Market Making with Scaled Beta Policies. arXiv:2207.03352.
- Guo, H., Lin, J. (2023). Market Making with Deep RL from Limit Order Books. arXiv:2305.15821.
- Fernández Vicente, Ó. (2025). Market Making Strategies with Reinforcement Learning. arXiv:2507.18680.
- Zimmer, R., Costa, O. (2025). RL-Based Market Making as a Stochastic Control on Non-Stationary LOB Dynamics. arXiv:2509.12456.
- Byrd, D., Hybinette, M., Balch, T. (2020). ABIDES. *ACM SIGSIM-PADS*.
- Amrouni, S. et al. (2021). ABIDES-Gym. *ICAIF*.

**Rigor experimental**
- Henderson, P. et al. (2018). Deep Reinforcement Learning that Matters. *AAAI*.
- Agarwal, R. et al. (2021). Deep RL at the Edge of the Statistical Precipice. *NeurIPS*.
- Bailey, D., López de Prado, M. (2014). The Deflated Sharpe Ratio. *J. Portfolio Management*.

> Verificar en las fuentes originales todos los datos bibliográficos (año, revista, páginas) antes de incluirlos en la memoria.
