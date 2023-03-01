import pickle

if __name__=="__main__":
    with open("a.pickle", "rb") as f:
        # a = str.encode(f.read())
        b = pickle.loads(f.read())
        b("FDg")