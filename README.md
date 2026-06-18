# Immo Eliza – Real Estate Price Prediction Pipeline


**Repository:** immo-eliza-scraping
**Type:** Consolidation & Collaboration
**Duration:** Sprint 1: 5 days
**Deadline:** Sprint 1: 19/06/2026, 4:00 pm
**Team:** 4 teammates

---

## 🧠 Mission Objective

The real estate company "Immo Eliza" wants to develop a machine learning model to make price predictions on real estate sales in Belgium. They hired you to help with the entire pipeline. Immovlan.be is a commonly used website for Belgian properties.

Your first task is to build a dataset that gathers information about at least 10000 properties all over Belgium. This dataset will be used later to train your prediction model.

## 🎓 Learning Objective

- Use Python to collect as much data as possible.
- At the end of this (sub)project, you will:
- Be able to scrape a website
- Be able to build a dataset from scratch
- Collaborate in a team using GitHub Projects
- Use Git in a team setting

## ⚙️ Installation

- pip install -r requirements.txt

## ▶️ Usage
- 🚀How to run the scraper — which file to execute and the command
-  **What it does?** — Scrapes property listings from Immovlan
- 📤Output — where the results are saved (e.g. a CSV file)
- 🔧Any configuration — like changing search parameters, number of threads, etc.


## 📦 Repo Architecture & Git Flow

```
immoeliza-scraping/
├── .gitignore
├── 📄README.md
├── 📄requirements.txt
├── 🚀main.py
├── 📁dev/
│   ├── 
│   └── 
└── 📁src/
    ├── __init__.py
    ├── 
    └── 
```
## 🌿 Git flow and branching strategy 

- **Protected Branches: * main:** Only contains functional, completed code. No one commits directly to main.
- **dev:** The integration branch where team members merge their features to test them together.
- **Feature Branches:** Every new task gets its own branch stemming from dev. - **Use a naming convention:** feature/your-name-task-description (e.g., feature/sam-url-scraper).
- **The PR & Merge Protocol:**
Pull the latest changes from dev before starting.
- Write code on your local feature branch.
- Push your branch to GitHub and open a - Pull Request (PR) targeting the dev branch.
- **Rule of Two:** At least one other team member (preferably the Git Commander) must review the code and approve the PR before it is merged into dev.
- Once dev is completely stable and the 10,000+ dataset is generated, make one final PR from dev into main.

## 👥Collaboration Structure

- Assigning Roles To ensure accountability and smooth collaboration, every team member must take on one of these core roles. 
- **Project Lead (Agile Master):** Manages the GitHub Project board, ensures deadlines are met, keeps meetings short, and helps unblock team members.
- **Git Commander (Repo Manager):** Sets up the repository, enforces the branching strategy, reviews Pull Requests (PRs), and resolves nasty merge conflicts.
- **Documentation Specialist:** Leads the creation of a stellar README.md, documents data dictionaries, and structures the final presentation.
- **QA & Data Architect (1-2 people):** Defines the final data structure (CSV/JSON schema), ensures data types are consistent, and checks for duplicates or missing values during data consolidation.

## 📌 Project Description & Goal

**Immo Eliza** is a data pipeline project built for a Belgian real estate company looking to develop a machine learning model to predict property sale prices across Belgium.

The project is structured as a multi-sprint pipeline:
- **Sprint 1 (current):** Scrape and collect a dataset of at least 10,000 Belgian property listings
- **Sprint 2 (upcoming):** Data analysis & cleaning
- **Sprint 3 (upcoming):** Machine learning model for price prediction

The data is sourced from [Immovlan.be](https://immovlan.be/en), one of Belgium's most widely used real estate platforms.

## 📚 Sources
- (https://immovlan.be/)- data source
- (https://www.immoscoop.be/en/) (data source)


## 📸 Visuals

**Terminal Output**


**Sample of CSV File**

**Data Preview**

| property_id | property_type | property_subtype |price | price_type | living_area_m2 |
|------|------|----------|------|------|----------|
| https://immovlan.be/en/detail/residence/for-sale/6110/montigny-le-tilleul/rbw17451| House | residence| 497000| sell| 210|
| https://immovlan.be/en/detail/residence/for-sale/9090/melle/rbw20118| House | residence| 208000| sell| 112|
https://immovlan.be/en/detail/residence/for-sale/4602/cheratte/vbe34783| House | residence| 280000| sell| 120|
https://immovlan.be/en/detail/penthouse/for-sale/8670/koksijde/rbw19038| Apartment | penthouse| 975000| sell| 116|
https://immovlan.be/en/detail/studio/for-sale/1180/ukkel/rbw19605| Apartment | studio| 155000| sell| 54|
https://immovlan.be/en/detail/residence/for-sale/6044/roux/vbe34825| House | residence| 139999| sell| 85|275000 | Antwerp | 2 |
| 350000 | Brussels | 3 |
| 275000 | Antwerp | 2 |












## 👥 Contributors

- Danukendi
- Irene
- Neha
- Victor


















Team 1: Dan (Agile Master), Irene (Repo Manager), Neha (Documentation Specialist), Victor (QA &amp; Data Architect)
"Immo Eliza Scraping" 







## 👥 Contributors

- Danukendi
- Irene
- Neha
- Victor


















Team 1: Dan (Agile Master), Irene (Repo Manager), Neha (Documentation Specialist), Victor (QA &amp; Data Architect)
"Immo Eliza Scraping" 
