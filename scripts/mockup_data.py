import csv
import random
from faker import Faker

faker = Faker()
teams = ["CSK", "MI", "RCB", "SRH", "DC", "KKR", "GT", "LSG", "PBKS", "RR"]



def mockup_data(num_of_data=1000):
    match_data = []
    for _ in range(num_of_data):
        match_id = faker.random_digit_above_two()
        team1,team2 = random.sample(teams,2)
        winner = random.choice([team1,team2])
        toss_winner = random.choice([team1,team2])
        match_data.append((
            match_id,
            faker.name(),
             team1,
             team2,
             winner,
            toss_winner,
            faker.city(),
             faker.date_between(start_date="-1y", end_date="today"))
        )
    return match_data

def write_to_csv(file_name, data):
    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["match_id", "captain", "team1", "team2", "winner", "toss_winner", "venue", "date_of_match"])
        for row in data:
            writer.writerow(row)
    return file_name


def generate_mockup_data(num_of_data=1000,file_name="mockup_data.csv"):
    data = mockup_data(num_of_data)
    file_name = write_to_csv(file_name=file_name,data=data)
    return file_name