import json
with open('./e2e-pipeline-main/grafana-provisioning/dashboards/edgex_system_monitoring.json', 'r', encoding='utf-8') as f:
    dashboard = json.load(f)

for panel in dashboard.get('panels', []):
    for target in panel.get('targets', []):
        if 'rawSql' in target:
            sql = target['rawSql']
            target['query'] = sql
            target['queryText'] = sql
            target['sql'] = sql

if 'templating' in dashboard and 'list' in dashboard['templating']:
    for var in dashboard['templating']['list']:
        if 'query' in var and isinstance(var['query'], str):
            q = var['query']
            var['query'] = {"query": q, "rawSql": q, "queryText": q, "sql": q}

with open('d:\\e2e-pipeline-main\\e2e-pipeline-main\\grafana-provisioning\\dashboards\\edgex_system_monitoring.json', 'w', encoding='utf-8') as f:
    json.dump(dashboard, f, indent=2, ensure_ascii=False)
print("Updated dashboard JSON")
