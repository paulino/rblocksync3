.PHONY:  package clean

package: clean
# 	sudo apt-get build-dep .
	dpkg-buildpackage -us -uc

clean:
	rm -rf debian/.debhelper debian/debhelper-build-stamp debian/files debian/*.substvars debian/rblocksync3
	rm -f rblocksync3.1 debian/*.log
	rm -f rblocksync3