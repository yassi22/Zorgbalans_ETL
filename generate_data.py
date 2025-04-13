import random
import pandas as pd
import json
from datetime import datetime, timedelta


# Utility Functions
def generate_random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)


def format_date(date):
    return date.strftime("%d-%m-%Y") if isinstance(date, datetime) else date


# Employee Data Generation Functions
def generate_employees(num_employees, start_date, min_hire_date, max_hire_date, locations, contract_options,
                       in_training_options, expertise_options, gender_options, expertise_weights,
                       low_probability_locations, medium_probability_locations, other_locations,
                       male_favored_weights, female_favored_weights, contract_weights, in_training_weights):
    employees = []
    # counts = {"low": 0, "medium": 0, "high": 0}  # Initialize counters
    for _ in range(num_employees):
        age, hire_date = generate_age_and_hire_date(start_date, min_hire_date, max_hire_date)
        termination_date = generate_termination_date(hire_date)
        location = generate_location(low_probability_locations, medium_probability_locations, other_locations, employees) #, counts, num_employees)
        contract_type = random.choices(contract_options, contract_weights, k=1)[0]
        in_training = random.choices(in_training_options, in_training_weights, k=1)[0]
        expertise, gender = generate_expertise_and_gender(expertise_options, gender_options, expertise_weights, male_favored_weights, female_favored_weights)

        employees.append({
            "gender": gender,
            "age": age,
            "contract_type": contract_type,
            "expertise": expertise,
            "hire_date": hire_date,
            "termination_date": termination_date,
            "work_location": location,
            "in_training": in_training
        })
    return employees



def generate_age_and_hire_date(start_date, min_hire_date, max_hire_date):
    age = random.choices(range(18, 65), weights=[1 if x < 30 else 2 if x < 50 else 3 for x in range(18, 65)], k=1)[0]
    while True:
        hire_date = generate_random_date(min_hire_date, max_hire_date)
        birth_year = start_date.year - age
        if birth_year + 18 <= hire_date.year:
            return age, hire_date


def generate_termination_date(hire_date):
    if random.random() < 0.3:
        return generate_random_date(hire_date, datetime(2024, 12, 31))
    return 0


def generate_location(low_probability_locations, medium_probability_locations, other_locations, current_employees):
    # Definieer de kansverdeling
    categories = ["low", "medium", "high"]
    weights = [0.055, 0.18, 0.765]  # 5% kans op low, 18% op medium, 77% op high
    for low_probability_location in low_probability_locations:
        amount_of_location_in_employees = 0
        for employee in current_employees:
            if employee["work_location"] == low_probability_location:
                amount_of_location_in_employees += 1
        if amount_of_location_in_employees <= 5:
            return low_probability_location

    # Kies een categorie op basis van de gewichten
    selected_category = random.choices(categories, weights=weights, k=1)[0]

    # Kies een specifieke locatie uit de geselecteerde categorie
    if selected_category == "low":
        return random.choice(low_probability_locations)
    elif selected_category == "medium":
        return random.choice(medium_probability_locations)
    else:
        return random.choice(other_locations)



def generate_expertise_and_gender(expertise_options, gender_options, expertise_weights, male_favored_weights, female_favored_weights):
    expertise = random.choices(expertise_options, weights=expertise_weights, k=1)[0]
    if expertise in ["IT", "Financieel", "Techniek"]:
        gender = random.choices(gender_options, weights=male_favored_weights, k=1)[0]
    elif expertise in ["HR", "Administratie", "Zorg"]:
        gender = random.choices(gender_options, weights=female_favored_weights, k=1)[0]
    else:
        gender = random.choice(gender_options)
    return expertise, gender


# Absence Tracking Function
def track_absences(employees, start_date, end_date, low_probability_locations, medium_probability_locations):
    rows = []
    for employee in employees:
        current_date = start_date
        while current_date <= end_date:
            if current_date < employee["hire_date"] or (
                    employee["termination_date"] != 0 and current_date > employee["termination_date"]):
                pass
            else:
                rows.extend(process_employee_absence(employee, current_date, end_date, low_probability_locations, medium_probability_locations))
            current_date += timedelta(days=1)
    return rows



def process_employee_absence(employee, current_date, end_date, low_probability_locations, medium_probability_locations):
    rows = []

    # Base absence probability
    base_absence_prob = 1 / 200

    # Adjust probability for gender
    if employee["gender"] == "V":
        base_absence_prob *= 1.2  # 15% more likely for women

    # Adjust probability for contract type
    if employee["contract_type"] in ["Fulltime", "Parttime"]:
        base_absence_prob *= 1.25  # 10% more likely for Fulltime/Parttime

    # Adjust probability for expertise
    if employee["expertise"] == ["Zorg"]:
        base_absence_prob *= 1.2  # 10% more likely for Fulltime/Parttime

    # Adjust probability for in_training status
    if employee["in_training"] == "ja":
        base_absence_prob *= 1.3  # 30% more likely for in training

    if employee["age"] >= 50:
        base_absence_prob *= 1.2 # 20% more likely for ages above 50

    # Adjust probability for location
    location_category = (
        "low" if employee["work_location"] in low_probability_locations else
        "medium" if employee["work_location"] in medium_probability_locations else
        "high"
    )
    if location_category == "high":
        base_absence_prob *= 1.15  # 10% more likely for high-probability locations

    # Determine absence
    if random.random() < base_absence_prob:
        absence_duration = random.randint(1, 7)
        for day in range(absence_duration):
            absence_date = current_date + timedelta(days=day)
            if absence_date > end_date or (
                    employee["termination_date"] != 0 and absence_date > employee["termination_date"]):
                break
            rows.append(record_employee_data(employee, absence_date, is_absent=1))
    else:
        rows.append(record_employee_data(employee, current_date, is_absent=0))
    return rows



def record_employee_data(employee, date, is_absent):
    return [
        employee["gender"], employee["age"], employee["contract_type"], employee["in_training"], is_absent,
        employee["expertise"], format_date(employee["hire_date"]), format_date(date),
        format_date(employee["termination_date"]) if employee["termination_date"] != 0 else 0, employee["work_location"]
    ]


# Main Program
def main():
    num_employees = 3000
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    min_hire_date = datetime(1999, 1, 1) #oprichting zorgbalans
    max_hire_date = datetime(2024, 12, 31)

    # Load locations from JSON file
    with open('work_locations.json', 'r') as file:
        locations_data = json.load(file)
    locations = locations_data["locations"]

    low_probability_locations = [loc for loc in locations if
                                 loc.startswith(("Buurtteam", "Prettig Thuisteam", "Huis"))]
    medium_probability_locations = [loc for loc in locations if
                                  loc.startswith(("Ontmoetingscentrum", "Dagcentrum"))]
    other_locations = [loc for loc in locations if loc not in low_probability_locations and loc not in medium_probability_locations]

    expertise_options = ["HR", "Administratie", "IT", "Financieel", "Zorg", "Techniek"]
    expertise_weights = [1, 1, 1, 1, 3, 1]

    gender_options = ["M", "V"]
    male_favored_weights = [3, 2]
    female_favored_weights = [2, 3]

    contract_options = ["Fulltime", "Parttime", "vrijwilliger", "Stage"]
    contract_weights = [2, 2, 1, 1]

    in_training_options = ["ja", "nee"]
    in_training_weights = [1, 3]


    employees = generate_employees(
        num_employees, start_date, min_hire_date, max_hire_date, locations, contract_options, in_training_options,
        expertise_options, gender_options, expertise_weights, low_probability_locations, medium_probability_locations, other_locations, male_favored_weights,
        female_favored_weights, contract_weights, in_training_weights,
    )
    rows = track_absences(employees, start_date, end_date, low_probability_locations, medium_probability_locations)

    columns = [
        "Geslacht", "Leeftijd", "Dienstverband/uren contract", "In opleiding", "Verzuim", "Deskundigheid",
        "Datum in dienst", "Datum", "Datum uit dienst", "Werklocatie"
    ]
    df = pd.DataFrame(rows, columns=columns)
    output_file = "verzuim_data.xlsx"
    df.to_excel(output_file, index=False)
    print(f"Data succesvol gegenereerd en opgeslagen in {output_file}.")


if __name__ == "__main__":
    main()

