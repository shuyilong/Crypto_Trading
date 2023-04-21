import tensorflow as tf
import numpy as np

def build_model(state_size, action_size, learning_rate):
    model = tf.keras.Sequential()
    model.add(tf.keras.layers.Dense(64, input_dim=state_size, activation='relu'))
    model.add(tf.keras.layers.Dense(64, activation='relu'))
    model.add(tf.keras.layers.Dense(action_size, activation='linear'))
    #model.compile(loss='mse', optimizer=tf.keras.optimizers.Adam(lr=learning_rate))
    return model

def initialize_weights(state_size,action_size,learning_rate):
    network = build_model(state_size=state_size, action_size=action_size,
                                    learning_rate=learning_rate)
    network.build()
    weights = [tf.zeros_like(w) for w in network.get_weights()]
    network.set_weights(weights)
    return network