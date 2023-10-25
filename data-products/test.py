import requests


TOKEN = "dbtc_fYE-k8McsXns5kYcNL3ea3905N5ujf0Qbd7Mj1dYn5AtsT07oA"

HEADERS = {"x-Accept": "application/json", "Authorization": f"Bearer {TOKEN}"}


url = "https://cloud.getdbt.com/api/v2/accounts/4171/runs/?job_definition_id=52822&project_id=7569&status=1"


data = requests.get(url=url, headers=HEADERS)

runs = [x["id"] for x in data.json()["data"]]

for r in runs:
    cancel_url = f"https://cloud.getdbt.com/api/v2/accounts/4171/runs/{r}/cancel/"
    p = requests.post(url=cancel_url, headers=HEADERS)
    print(p.json())
