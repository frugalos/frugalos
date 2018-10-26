.PHONY: apidoc apidoc-clean

.SUFFIXES: .md .html

AGLIO=./node_modules/.bin/aglio

MV=mv

RM=rm -f

APIDOC_SRC:=$(wildcard apidoc/v1/*.md)

APIDOCS=$(APIDOC_SRC:.md=.html)

apidoc: $(APIDOCS)
	$(MV) $^ docs/v1/

apidoc-clean:
	$(RM) $^ $(APIDOCS)

%.html: %.md
	$(AGLIO) -i $< -o $@
