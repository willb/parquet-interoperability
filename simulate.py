import heapq
import numpy as np

def simulate(event_generators, initial_time=0):    
    def setup_e(e, i):
        offset, result = next(e)
        return ((offset + i), result, e)
    
    pq = [setup_e(event, initial_time)
          for event in event_generators]
    heapq.heapify(pq)
    
    while True:
        timestamp, result, event = pq[0]
        offset, next_result = event.send(timestamp)
        heapq.heappushpop(pq, (timestamp + offset, next_result, event))
        yield (timestamp, *result)


neighborhoods = ["Mitte", "Moabit", "Hansaviertel", "Tiergarten", "Wedding", "Gesundbrunnen", "Friedrichshain", "Kreuzberg", "Prenzlauer Berg" "Weißensee", "Blankenburg", "Heinersdorf", "Karow", "Stadtrandsiedlung Malchow" "Pankow", "Blankenfelde", "Buch", "Französisch Buchholz", "Niederschönhausen" "Rosenthal", "Wilhelmsruh"]

def basic_user_stream(user_id, mu, seed=None):  
    favorite_neighborhoods = np.random.choice(neighborhoods, size=len(neighborhoods) // 10)
    while True:
        amount = np.random.randint(75, 2000) / 100
        if np.random.randint(0, 10) > 8:
            neighborhood, = np.random.choice(neighborhoods, size=1)
        else:
            neighborhood, = np.random.choice(favorite_neighborhoods, size=1)
        offset = np.random.poisson(lam=mu)
        result = {
            "user_id": user_id,
            "amount": amount,
            "neighborhood": neighborhood
        }
        yield (offset, (True, *result.values()))


results = []
simulation = simulate([basic_user_stream(x, ((x % 25) + 10) * 100) for x in range(512)])
for i in range(1000000):
    current = next(simulation)
    results.append({"timestamp": current[0], "user_id": current[2], "amount": current[3], "neighborhood": str(current[4])})
