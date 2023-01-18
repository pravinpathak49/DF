import time

def time_it(func):
	def inner_func(*args):
		start = time.time()
		result = func(*args)
		end = time.time()
		print(func.__name__ + " took "+ str((end - start)*1000) +"mili sec.")
		return result
	return inner_func