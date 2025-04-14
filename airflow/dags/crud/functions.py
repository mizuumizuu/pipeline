def parse_scripts(path: str, raw: bool = False) -> list[str]:
    import os
    
    scripts_list_raw = os.listdir(path)
    if raw:
        return scripts_list_raw
    
    scripts_list_parsed = []
    
    for item in scripts_list_raw:
        scripts_list_parsed.append(item.split('__')[1].rstrip('.sql'))
        
    return scripts_list_parsed
SQL_SCRIPTS_PATH='/opt/airflow/dags/sql'
parse_scripts(SQL_SCRIPTS_PATH)