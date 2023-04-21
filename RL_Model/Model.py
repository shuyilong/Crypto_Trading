import random
import numpy as np
import RL_Model as RM
import tensorflow as tf

### Hyper parameters
currency = ['BTC']
interval = 120
begin_date='2022-10-02'
end_date='2022-10-05'

### 1. Set initialization parameters
epi = RM.initial_epi_rate()
gamma = RM.discount_rate()
action_space = RM.action_space(1)
action_size = len(action_space)
learning_rate = RM.initial_learning_rate()
max_iteration_num = RM.max_iteration_num()
decay_rate = RM.decay_rate()
stop_rate = RM.stop_rate()

experience_pool = RM.experience_pool(currency, interval, begin_date, end_date)
state_size = len(experience_pool['state'].iloc[0])+1
batch_size = int(len(experience_pool) / 20)

### 2. initialize target network AND q network
target_network = RM.initialize_weights(state_size,action_size,learning_rate=learning_rate)
q_network = RM.initialize_weights(state_size,action_size,learning_rate=learning_rate)


### 3. Iteration

for epoch in range(20):
    replay = RM.Replay(experience_pool, num = batch_size)
    state_list,loss_list = [],[]
    target_q = np.zeros((batch_size, action_size))
    for i in range(batch_size):
        last_action = random.choice(action_space)
        state = replay['state'].iloc[i]
        action = RM.action_calculate(state, action_space, q_network, epi)
        reward = [RM.reward_calculate(replay['reward'].iloc[i], last_action, action) for action in action_space]
        next_state = np.array(replay['next_state'].iloc[i] + [action]).reshape(1, -1)

        target_q_value = [ret + gamma * np.amax(target_network.predict(next_state)[0]) for ret in reward]
        target_q[i, :] = target_q_value
        state_list.append(state[0])

    state_list_np = np.array(state_list)
    q_network.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.1),
                      loss='mse')
    print("Epoch : ", epoch)
    history = q_network.fit(x=state_list_np, y=target_q, epochs=1, verbose=1)
    loss_list.append(history.history['loss'][-1])
    target_network.set_weights(q_network.get_weights())
    epi = epi*(1-decay_rate)
    #if np.mean(loss_list[-5:]) <= stop_rate :
    #    break



