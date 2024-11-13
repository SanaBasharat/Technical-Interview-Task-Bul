
# Technical Task

Python and SQL based Data Engineering task.

## Installation
1. Clone the repo
```bash
git clone https://github.com/github_username/repo_name.git
```
2. Create a virtual environment
```bash
python -m venv venv
```
3. Activate the environment
<ul> For Linux based OS Or Mac-OS</ul>

```bash
source venv/bin/activate
```
<ul> For Windows with CMD</ul>

```bash
.venv\Scripts\activate
```
4. Install requirements
```bash
pip install -r requirements.txt
```
## Usage
1. Data Extraction module:
Provide space-separated list of station IDs and years
```py
python data_extraction.py [-h] -s STATIONS [STATIONS ...] -y YEARS [YEARS ...]
python data_extraction.py -s 26953 31688 -y 2024 2023 2022
```
2. Data Transformation module:
```py
python data_transformation.py [-h] [-n FILENAME] [-t FILETYPE]
python data_transformation.py -n final_data -t csv
```
