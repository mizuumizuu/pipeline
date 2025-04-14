from pathlib import Path

SQL_SCRIPTS_PATH = Path(__file__).parent / 'sql'

def script_parsing(path: str) -> list[str]:
    import os
    list_of_scripts = os.listdir(path)
    
    final_scripts = []
    
    for script in list_of_scripts:
        script_shortened = Path(script).stem
        final_script_name = script_shortened.split('__')[-1]
        final_scripts.append(final_script_name)

    return final_scripts

#rez = script_parsing(SQL_SCRIPTS_PATH)
import os
print(os.listdir('./')) 
rez = script_parsing('./sql') 
print(rez)

