import RL_Model as RM

def reward_calculate(ret, last_action, action):
    return (1 + ret * action) * (1 - abs(action - last_action) * RM.cost()) - 1