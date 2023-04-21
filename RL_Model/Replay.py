import numpy as np

def Replay(experience_pool, num = 100):
    replay = experience_pool.sample(n=num)
    replay['last_action'] = replay['last_action'].apply(lambda x : [x])
    replay['state'] = replay['state'] + replay['last_action']
    del replay['last_action']
    replay['state'] = replay['state'].apply(lambda x : np.array(x).reshape(-1, len(replay['state'].iloc[0])))
    #replay['next_state'] = replay['next_state'].apply(lambda x : np.array(x).reshape(-1, len(replay['next_state'].iloc[0])))
    return replay