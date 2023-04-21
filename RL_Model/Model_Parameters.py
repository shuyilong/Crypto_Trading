def initial_learning_rate():
    return 0.1
def cost():
    return 0.0003

def initial_epi_rate():
    return 0.1

def decay_rate():
    return 0.002

def discount_rate():
    return 0.9

def max_iteration_num():
    return 200

def stop_rate():
    return 0.001

def action_space(objective_num=1):
    if objective_num == 1:
        return [-1,0,1]
    else:
        raise ValueError('Haven\'t finish')
