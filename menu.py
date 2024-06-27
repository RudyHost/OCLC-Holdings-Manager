import cli_ui
import glob
from ohm_settings import OhmSettings
from ohm_database import OhmDatabase
from ohm_marc import OhmMarc
from ohm_oclc import OhmOclc
import json
import os

adds_sorted = {}
deletes_sorted = {}
resume_data = {}
settings_file = None

def check_resume():
    if not os.path.isfile('resume.json'):
        return False
    with open("resume.json") as resume_file:
        resume_data = json.load(resume_file)
        resume_file.close()
    for library in resume_data["adds"]:
        if len(library) > 0:
            return True
    for library in resume_data["deletes"]:
        if len(library) > 0:
            return True
    return False

def sort_changes(changes_list):
    changes = {}
    for entry in changes_list:
        if entry[0] not in changes.keys():
            changes[entry[0]]=[entry[1]]
        else:
            changes[entry[0]].append(entry[1])
    return changes



cli_ui.info_1("Welcome to OCLC Holdings Manager")

if check_resume():
    answer = input("Interupted session found, resume? (y/n)").lower()
    if answer[0] == "y":
        resume_data = json.load(open('resume.json'))
        settings_file = resume_data["settings_file"]
        adds_sorted = resume_data["adds"]
        deletes_sorted = resume_data["deletes"]


# Read in the settings
if not settings_file:
    settings_files = glob.glob('settings*.json')
    settings_file = cli_ui.ask_choice("Which settings file should I use?", choices=settings_files, sort=True)
settings = OhmSettings(settings_file)
resume_data["settings_file"] = settings_file

# load sqlite3 database
database = OhmDatabase(settings.database)

# initialize OCLC API
oclc_conn = OhmOclc(settings.oclc_credentials)

menu_items = ("Parse MARC extract", "Compare changes", "Send to OCLC", "Analyze Results","Test OCLC WSKey", "Exit")

while True:
    menu_choice = cli_ui.ask_choice("OHM Main Menu", choices=menu_items, sort=False)

    if menu_choice == "Parse MARC extract":
        extracts_files = glob.glob(f'extracts/{settings.extract_naming_scheme}')
        extracts_files.sort()

        extract_file = cli_ui.ask_choice("Which extract file should I use?", choices=extracts_files)
        print(f'Using {extract_file}')

        table_name = cli_ui.ask_string("What should I name the table?", extract_file)

        parse_marc = OhmMarc(database, settings, extract_file, table_name)
        parse_marc.parse_marc_file()

    elif menu_choice == "Compare changes":
        # get list of tables
        tables = database.list_tables()
        current_data = cli_ui.ask_choice("Which is the latest data?", choices=tables, sort=False)
        tables.remove(current_data)
        previous_data = cli_ui.ask_choice("Which is the last run's data?", choices=tables, sort=False)

        print(f'Comparing {current_data} to {previous_data}')

        adds = database.compare_tables(current_data, previous_data)
        deletes = database.compare_tables(previous_data, current_data)

        print(f'{len(adds)} Adds, {len(deletes)} Deletes')

        adds_sorted = sort_changes(adds)
        deletes_sorted = sort_changes(deletes)

        # Resume support
        resume_data["adds"] = adds_sorted
        resume_data["deletes"] = deletes_sorted
        json.dump(resume_data, open('resume.json', 'w'))

        #Remove unsorted changes
        del adds, deletes

    elif menu_choice == "Send to OCLC":
        if len(adds_sorted) == 0 and len(deletes_sorted) == 0:
            print("Please compare changes first.")
        else:
            for institution in deletes_sorted:
                for oclc_num in deletes_sorted[institution]:
                    print(f'UNSET {institution}: {oclc_num}')
                    oclc_conn.unset_holding(oclc_num, institution)
            for institution in adds_sorted:
                for oclc_num in adds_sorted[institution]:
                    print(f'SET {institution}: {oclc_num}')
                    oclc_conn.set_holding(oclc_num, institution)

    elif menu_choice == "Analyze Results":
        results_directories = glob.glob('*results')
        directory = cli_ui.ask_choice("Which directory?", choices=results_directories)
        file_prefix = cli_ui.ask_string("What should I use as a prefix for the output files?")
        oclc_conn.analyze_results(results_directory = directory, file_prefix = file_prefix)

    elif menu_choice == "Test OCLC WSKey":
        failed_symbols = oclc_conn.test_wskey(settings.holding_map)

        if len(failed_symbols) > 0:
            print("The following symbols are misconfigured for this WSKey:")
            for symbol in failed_symbols:
                print(f'{symbol}: {failed_symbols[symbol]}')
        else:
            print("OCLC WSKey configured properly for all symbols.")

    elif menu_choice == "Exit":
        break
    