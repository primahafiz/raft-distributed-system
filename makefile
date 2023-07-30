api:
	python3 src/api.py $(PORT)

client:
	python3 src/initclient.py localhost $(PORT)

raft:
	python3 src/initraft.py localhost $(PORT) localhost $(CONTACT_PORT)