import random
import numpy as np

def action_calculate(state, action_space, q_network, epi):
    if random.random() < epi:
        return random.choice(action_space)
    else:
        return action_space[np.argmax(q_network.predict(state))]