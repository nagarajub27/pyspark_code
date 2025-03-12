def my_generator():
    for i in range(5):
        yield i

# Create a generator object
gen = my_generator()

# Iterate through the generator
for value in gen:
    print(value)
# Output: 0 1 2 3 4
