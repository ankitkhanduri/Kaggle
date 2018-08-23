
# coding: utf-8

# In[15]:


import time
def special_sum(lst):
    start_time = time.clock()
    output_list = []
    for i, position in enumerate(lst):
        total = 0
        for j, number in enumerate(lst):
            if j == i:
                continue
            else:
                total = total + number
        output_list.append(total)
    print (time.clock() - start_time, "seconds")
    return output_list



# In[16]:


special_sum([1,2,3,4])


# In[17]:


import time
def special_sum(lst):  
    start_time = time.clock()   
    sum_ = sum(lst)
    output_list = [sum_ - x for x in lst]
    print (time.clock() - start_time, "seconds")
    return output_list


# In[18]:


special_sum([1,2,3,4])

