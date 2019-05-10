PRODUCT = connection_pool
BUILD_DIR = $(HOME)/build
PROD_DIR = $(BUILD_DIR)/$(PRODUCT)
TAR_DIR = /tmp/$(USER)
TAR_FILE = $(TAR_DIR)/$(PRODUCT)_$(VERSION).tar

FILES = Version.py ConnectionPool.py __init__.py LICENSE

all:
	make VERSION=`python Version.py` all_with_version_defined
	
clean:
	rm -rf $(PROD_DIR) $(TAR_FILE)

all_with_version_defined:	tarball
	
    
build: $(PROD_DIR)
	cp $(FILES) $(PROD_DIR)
    
tarball: clean build $(TAR_DIR)
	cd $(BUILD_DIR); tar cf $(TAR_FILE) $(PRODUCT)
	@echo 
	@echo Tar file $(TAR_FILE) is ready
	@echo 


$(PROD_DIR):
	mkdir -p $@
    
$(TAR_DIR):
	mkdir -p $@

