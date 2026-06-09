import json
import os

filepath = 'd:/e2e-pipeline-main/e2e-pipeline-main/grafana-provisioning/dashboards/edgex_system_monitoring.json'
with open(filepath, 'r', encoding='utf-8') as f:
    data = json.load(f)

def replace_query(q):
    if not isinstance(q, str): return q
    q = q.replace("resource_name = 'CPUUsage'", "resource_name = 'cpu_data'")
    q = q.replace("resource_name = 'Temperature'", "resource_name = 'gpu_data'")
    q = q.replace("resource_name = 'MemUsed'", "resource_name = 'mem_data'")
    q = q.replace("resource_name = 'Uptime'", "resource_name = 'sys_data'")
    q = q.replace("resource_name = 'ProcessCount'", "resource_name = 'proc_data'")
    q = q.replace("resource_name IN ('LoadAvg1', 'LoadAvg5', 'LoadAvg15')", "resource_name = 'sys_data'")
    q = q.replace("resource_name = 'CPUFreq'", "resource_name = 'cpu_data'")
    q = q.replace("resource_name IN ('MemUsed', 'MemFree')", "resource_name = 'mem_data'")
    q = q.replace("resource_name IN ('SwapIn', 'SwapOut')", "resource_name = 'mem_data'")
    
    # Let's also handle Network and Disk just in case they were mapped specifically
    q = q.replace("resource_name IN ('NetBytesRecv', 'NetBytesSent')", "resource_name = 'net_data'")
    q = q.replace("resource_name IN ('NetPacketsRecv', 'NetPacketsSent')", "resource_name = 'net_data'")
    q = q.replace("resource_name IN ('NetErrorsIn', 'NetErrorsOut')", "resource_name = 'net_data'")
    q = q.replace("resource_name IN ('NetDropIn', 'NetDropOut')", "resource_name = 'net_data'")
    q = q.replace("resource_name IN ('DiskReadBytes', 'DiskWriteBytes')", "resource_name = 'disk_data'")
    q = q.replace("resource_name IN ('DiskReadCount', 'DiskWriteCount')", "resource_name = 'disk_data'")
    
    return q

for panel in data.get('panels', []):
    if 'targets' in panel:
        for target in panel['targets']:
            for key in ['rawSql', 'query', 'queryText', 'sql']:
                if key in target:
                    target[key] = replace_query(target[key])
    
    title = panel.get('title')
    if title == 'CPU Temperature': panel['title'] = 'GPU Status'
    elif title == 'CPU Temperature Over Time': panel['title'] = 'GPU Status Over Time'
    elif title == 'Load Average (1 / 5 / 15 min)': panel['title'] = 'System Data (Sys)'
    elif title == 'CPU Frequency (MHz)': panel['title'] = 'CPU Data (Alt)'
    elif title == 'Memory Used / Free Over Time': panel['title'] = 'Memory Data Over Time'
    elif title == 'Swap In / Out': panel['title'] = 'Memory Metrics (Alt)'
    
with open(filepath, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2)

print('Dashboard updated successfully.')
