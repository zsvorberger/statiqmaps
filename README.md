StatIQ Maps

StatIQ Maps is a cycling-focused geospatial analytics platform that combines Strava ride data with custom road segmentation, mapping tools, and ride intelligence.

Live platform:
https://statiqmaps.com

Overview

StatIQ Maps turns ride data into structured, road-level insights.

The platform is built around a custom road indexing system that enables per-road analytics rather than just ride-level summaries. It is designed to evolve into a surface-aware, infrastructure-intelligent mapping platform for cyclists.

Current Capabilities
Strava Integration

OAuth authentication

Activity ingestion and caching

Lifetime statistics and filtered time-range summaries

Ride-level metrics and aggregation

Custom Road Indexing

Roads segmented at intersections

Unique per-road lookup system

Road-level stat aggregation

Structured foundation for future geospatial intelligence

Interactive Mapping

Dynamic web-based map interface

Road-level visualization

Layer toggling and map interaction tools

Near-Term Roadmap

The platform is actively expanding toward:

CNN-based road surface classification integration

Elevation modeling and road-level climb profiling

Surface-aware routing logic

Advanced road-level scoring and safety metrics

These components are being developed as part of the broader StatIQ ecosystem.

Technology Stack

Backend:

Python

Flask

Gunicorn

Frontend:

JavaScript

Interactive web mapping libraries

Infrastructure:

Strava API

Custom road dataset

AWS Lightsail deployment

Cloud-based storage for mapping assets

Sensitive credentials are managed via environment variables and are not stored in this repository.

Vision

StatIQ Maps is being built as a scalable road-level intelligence platform for cyclists.

The long-term goal is to combine ride analytics, mapping data, and machine learning to create a structured, surface-aware geospatial system that operates beyond traditional fitness tracking.

Author

Zach Vorberger
Mechanical Engineer | Geospatial Developer
