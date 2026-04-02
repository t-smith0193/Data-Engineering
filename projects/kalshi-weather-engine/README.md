# kalshi-weather-engine

---
This project is an attempt at building a real-time trading engine for weather-based temperature markets on Kalshi. It’s built on top of a data pipeline that ingests aviation weather data and Kalshi market prices, then tries to connect forecasts, observed conditions, and market behavior.

The infrastructure runs on an AWS EC2 (Linux) instance with Dockerized Airflow handling scheduled ingestion and transformations. Data is stored in S3 as a central data lake, where both raw and processed datasets (JSON, CSV, parquet) live. Batch workflows are used to understand how weather evolves throughout the day and how that maps to market outcomes.

On top of that, there’s a layer of event-driven AWS Lambda functions that react to new data in real time. These handle things like alerting, Slack updates, anomaly detection, and triggering downstream analysis. There’s also an OpenAI-powered component that takes NWS forecast discussions (AFDs), filters them down to KSFO-relevant signals, and turns them into structured forecasts mapped directly to Kalshi market buckets.

The goal is to combine batch modeling (Airflow + DuckDB + S3) with real-time reactions (Lambda + Slack) to continuously compare live weather and forecasts against current market pricing and create useful signals as they happen.
<br><br>

<img width="1489" height="754" alt="Diagram" src="https://github.com/user-attachments/assets/0668f7f3-deb8-4b37-a5cf-b7c8060eafad" />


## Why San Fransico Temperature Market?

I chose to focus on the San Francisco (KSFO) daily high temperature market because it’s fundamentally harder and more interesting than most other major city contracts.

Unlike places like New York, Chicago, or Miami, where temperature is often driven by large-scale weather systems, San Francisco sits in a coastal microclimate where small changes in conditions can have outsized impacts. The daily high at KSFO is heavily influenced by:

- Marine layer depth and timing  
- Onshore vs offshore flow  
- Cloud and fog burn-off timing  
- Coastal vs inland temperature divergence  

This makes the outcome much less deterministic and more sensitive to subtle forecast details, especially in National Weather Service (NWS) discussions.

From a modeling perspective, it's ideal for the following reasons:
- It’s harder to predict so there is more opportunity for edge  
- It forces deeper understanding of weather mechanics  
- It makes forecast interpretation (not just raw data) more valuable  

It’s also a less crowded market compared to NY/CHI/MIA temperature contracts, which tend to be more efficiently priced. The combination of structural complexity and lower competition makes KSFO a good testbed for building and iterating on a real-time trading system.

## Results & Findings

One of the main takeaways from this project is that trading the *distribution* of outcomes (i.e. volatility across temperature brackets) is more effective than trying to predict the exact daily high temperature.

Early on, I approached this like a point forecast problem, trying to predict whether KSFO would hit a specific bracket (e.g. 72–75 vs 76–79). In practice, that’s extremely difficult, especially in a coastal microclimate like San Francisco where small changes in marine layer timing or cloud burn-off can shift the outcome by several degrees.

What ended up working better is treating the market more like an options surface:

- Each temperature bracket behaves similarly to a strike  
- The distribution of probabilities across brackets resembles an implied volatility curve  
- Pricing inefficiencies tend to show up in how probability mass is distributed, not just the “most likely” outcome  

Instead of asking *“What will the high be?”*, the better question became:
> *“How is the market mispricing the shape of the distribution?”*

This leads to strategies that look much closer to volatility trading:

- Identifying when mid-range buckets are overpriced relative to tails  
- Recognizing when upper brackets are systematically underpriced during offshore flow setups  
- Trading YES/NO across multiple brackets to express relative value rather than outright direction  

There are also some parallels (and differences) with traditional options markets:

**Similarities:**
- Trading probability distributions rather than point outcomes  
- Opportunities to exploit mispriced “volatility” across strikes  
- Edge comes from understanding the underlying driver (weather vs price dynamics), not just price  

**Differences:**
- Temperature outcomes are one-sided (bounded by physics), not symmetric like returns  
- There’s no direct equivalent of put/call skew, but there is a structural bias:
  - Higher temperature brackets can become “safer” in certain regimes (e.g. strong offshore flow)
- Contracts settle to a single discrete outcome rather than a continuous payoff  

Another key insight is timing:

- As the day progresses, uncertainty collapses quickly  
- Lower and higher brackets become “worthless” at different speeds depending on conditions  
- There are opportunities to trade that decay, similar to how options lose value as expiry approaches  

Overall, the edge doesn’t come from perfectly predicting the weather, it comes from understanding how weather uncertainty evolves and how the market prices (or misprices) that uncertainty in real time.

## Kalshi Results

The system has shown strong early performance by focusing on trading the distribution of outcomes rather than making single-point predictions.

Across multiple days, the strategy consistently captures value by identifying mispriced temperature brackets and trading around them (both YES and NO positions), rather than trying to perfectly predict the final high. As uncertainty collapses throughout the day, positions in losing brackets decay rapidly, creating opportunities similar to options expiry dynamics.

The examples below show realized payouts across different days, with profits coming from correctly positioning across multiple brackets and exploiting how the market reprices as weather conditions evolve.

Overall, this approach, treating the market like a volatility surface rather than a directional bet, has been the most consistently profitable.  
<br><br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/1745f8aa-ffe7-4706-be04-1c22a28df143" width="500">
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/e534ac46-4136-4677-89c6-bd008bd348d7" width="320">
  <img src="https://github.com/user-attachments/assets/cf2cd850-0f13-4b3a-8c21-80734413c589" width="320">
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/51c6f6f6-c6e0-4d34-8dfc-01e633abb0e0" width="320">
  <img src="https://github.com/user-attachments/assets/9767f510-1a1a-424e-ba2b-57d9425027c2" width="320">
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/6ac57706-eefc-4bd5-816b-95907c9493c4" width="320">
  <img src="https://github.com/user-attachments/assets/29715e9d-e774-4917-bef6-5d7b635445a7" width="320">
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/33dc3c98-49da-407f-b23e-ee0eb8749f92" width="320">
</p>
