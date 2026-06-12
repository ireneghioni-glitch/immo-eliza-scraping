# Immo Eliza - Data Collection

- Repository: `immo-eliza-scraping`
- Type: `Consolidation`
- Duration: `5 days`
- Deadline: `18/07/2025 4:00 PM`
- Show and Tell: Every team will have one presenter
- Team: Team

## Learning Objectives

Use Python to collect as much data as possible.

At the end of this (sub)project, you will:
- Be able to scrape a website
- Be able to build a dataset from scratch
- Collaborate in a team using Trello
- Use Git in a team setting


## The Mission

The real estate company "Immo Eliza" wants to develop a machine learning model to make price predictions on real estate sales in Belgium. They hired you to help with the entire pipeline. [Immovlan.be](https://immovlan.be/enl) is a commonly used website for Belgian properties.

Your first task is to build a dataset that gathers information about at least 10000 properties all over Belgium. This dataset will be used later to train your prediction model.

## Start here
1. Assigning Roles
To ensure accountability and smooth collaboration, every team member must take on one of these core roles. Note: Everyone will still write scraping code!

- Project Lead (Agile Master): Manages the Trello board, ensures deadlines are met, keeps meetings short, and helps unblock team members.
- Git Commander (Repo Manager): Sets up the repository, enforces the branching strategy, reviews Pull Requests (PRs), and resolves nasty merge conflicts.
- Documentation Specialist: Leads the creation of a stellar README.md, documents data dictionaries, and structures the final presentation.
- QA & Data Architect (1-2 people): Defines the final data structure (CSV/JSON schema), ensures data types are consistent, and checks for duplicates or missing values during data consolidation.

2. Explore the mission
With your team members explore the Immovlan website, and discuss what is the type of information relevant to the mission that you would want to scrape? Are there any data issues you already for see? Make a list of the columns that you would want in your final dataset (along with their data types)


- Make a plan of attack with your team and break the project into smaller pieces (what are possible features you can assign to each team member). Take a moment to note it down in Github Projects (which you will use to keep track of the project status); and add your coach to your Repo as a collaborator (vriveraq). 

3. Repo strategy
- Protected Branches: * main: Only contains functional, completed code. No one commits directly to main.
- dev: The integration branch where team members merge their features to test them together.
- Feature Branches: Every new task gets its own branch stemming from dev. Use a naming convention: feature/your-name-task-description (e.g., feature/sam-url-scraper).
- The PR & Merge Protocol:
    - Pull the latest changes from dev before starting.
    - Write code on your local feature branch.
    - Push your branch to GitHub and open a Pull Request (PR) targeting the dev branch.
    - Rule of Two: At least one other team member (preferably the Git Commander) must review the code and approve the PR before it is merged into dev.
    - Once dev is completely stable and the 10,000+ dataset is generated, make one final PR from dev into main.



## Must-have features (for the dataset)

- The data should have properties across all Belgium
- There should be at minimum unique 10000 data points
- Missing information is initially encoded with `None`
- Whenever possible, record only numerical values (for example, instead of defining whether the kitchen is equipped using `"Yes"` or `"No"`, use binary values instead)
- Use appropriate and consistent column names for your variables (those will be key to training and understanding your model later on)
- No duplicates
- No empty rows

## Nice-to-Have Features (Data Engineering Superpowers)
1. Speed & Performance Optimization: Scraping 10,000+ pages sequentially can take hours. Accelerating this safely is a massive plus.
    - Asynchronous or Multi-Threaded Scraping: Implement Python's concurrent.futures (ThreadPoolExecutor) or asyncio/aiohttp to fetch multiple pages at once.
    - Session Persistence: Use requests.Session()

2. Multi-Source Enrichment (Cross-Referencing Websites)
 - Secondary Target Scraper: Build a secondary, lightweight scraper for another Belgian real estate site (like Century 21, Zimmo, or Era) to complement your dataset or validate the pricing structure.
 - Geo-Data API Enrichment: Use the postal_code column to pull in external metadata. For example, integrate a free API or dataset to add a province, region (Flanders/Wallonia/Brussels), or even average income per inhabitant of that postal code. This adds massive predictive power for the later ML phase.

3. Pipeline Resilience & Anti-Bot Stealth
- Websites change and defense mechanisms trigger. Making your scraper bulletproof is a true engineering feat.

Smart User-Agent & Header Rotation: Use the fake-useragent library to dynamically rotate browser identities so your team avoids getting blacklisted.
- Progress Checkpointing (Fault Tolerance): Write your orchestrator so that it logs successfully scraped URLs to a file. If your internet drops or Immovlan blocks you at item #6,500, the script should be able to resume from item #6,501 without restarting from zero.
- Graceful Error Handling: Implement try-except blocks that catch 404 Not Found or 503 Service Unavailable errors, logging them cleanly to a scraping_errors.log instead of crashing the whole pipeline.

4. Advanced Data Extraction & Geolocation
- Hidden API Extraction: Inspect the Network Tab in your browser's Developer Tools. Often, modern sites load data via internal JSON APIs. If you can locate Immovlan's internal API endpoints, you can fetch clean JSON payloads directly, bypassing HTML parsing entirely.
- Coordinates Capture: Extract the exact Latitude and Longitude if they are embedded in the page's metadata or script tags. Distance to major cities is a massive feature for real estate valuation.


## Coding tips
- Start small and test often! Start by scraping one property then figure out how to scale up. Once you've tested you code for a few properties, move on to 10, 100, 1000,... etc. 
- Python packages that will come in very handy: `requests`, `BeautifulSoup`, `Selinium` and `pandas`
  - You can use other scraping tools such as `scrapy` or `playwright`at your own risk.
  - Keep it light in terms of `pandas` tooling, we'll give you some time afterwards to dive deeper into it for the analysis and visualization part
- You can use concurrency (Python advanced, Bonus material) to increase the speed of data collection
- You might have to work around CAPTCHA and other measures that want to slow you down in the scraping process - be creative ;-)
- Commit regularly and often (with good commit messages!)

## Deliverables

1. Publish your source code on a GitHub repository:
    - Make a private repository first (`immo-eliza-scraping`), share it with your team and coaches
      - Make it public at the end of the project
    - Have a `src` folder with your Python modules for scraping (note: you can use OOP or functions. Classes are not mandatory for this project. )
    - Have a `dev` folder containing the your "exploratory work"
    - Have a `data` folder with the dataset - feel free to subdivide the folder (e.g. `raw`, `cleaned`)
    - Have a `README.md` file
    - Have a `main.py` file to run the scraper
    - Have a `requirements.txt` file
    - Have a `.gitignore` file

2. Write a convincing and clear README file, including following elements as you see fit:
   - Description
   - Installation
   - Usage
   - Sources
   - Visuals
   - Contributors
   - Timeline

3. Slides-deck: A short slide deck that presents your team, project approach/workflow, and final output: We will have project debrief and show and tell on Friday at 4:00 PM. Volunteers or randomly selected people will present their slides.

## Evaluation criteria

| Criteria       | Indicator                                  | Yes/No |
| -------------- | ------------------------------------------ | ------ |
| 1. Is complete | Contains a minimum of 10000 data points    |        |
|                | Contains data across whole Belgium         |        |
|                | The dataset has no empty rows              |        |
|                | There are few non-numeric values           |        |
|                | Your code is slick & clean                 |        |
|                | Repository and commit history is clear     |        |
| 2. Is great    | Used threading/multiprocessing             |        |

## Final note

_"Attempts to create thinking machines will be a great help in discovering how we think ourselves." - Alan Turing_

![You've got this!](https://i.giphy.com/media/JWuBH9rCO2uZuHBFpm/giphy.gif)
