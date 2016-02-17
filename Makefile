PROJECT = sumo_db_riak

DEPS = lager sumo_db riakc iso8601

dep_lager = git https://github.com/basho/lager.git 3.0.1
dep_sumo_db = git https://github.com/inaka/sumo_db.git de5998d56685
dep_riakc = git https://github.com/inaka/riak-erlang-client.git 2.1.1-R18
dep_iso8601 = git https://github.com/zerotao/erlang_iso8601.git 0d14540

include erlang.mk

LOCAL_DEPS := tools common_test crypto test_server
DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Wunmatched_returns

ERLC_OPTS := +'{parse_transform, lager_transform}'
ERLC_OPTS += +warn_unused_vars +warn_export_all +warn_shadow_vars +warn_unused_import +warn_unused_function
ERLC_OPTS += +warn_bif_clash +warn_unused_record +warn_deprecated_function +warn_obsolete_guard +strict_validation
ERLC_OPTS += +warn_export_vars +warn_exported_vars +warn_missing_spec +warn_untyped_record +debug_info

# Commont Test Config

TEST_ERLC_OPTS += +'{parse_transform, lager_transform}' +debug_info
CT_OPTS = -cover test/sumo.coverspec -vvv -erl_args -boot start_sasl -config ${CONFIG}

erldocs:
	erldocs . -o docs

changelog:
	github_changelog_generator --token ${TOKEN}

EDOC_OPTS += todo, report_missing_types

# Riak tests
CT_SUITES_RIAK = nested_docs
CT_OPTS_RIAK = -vvv -erl_args -config test/riak/riak_test.config

riak_tests: test-build
	mkdir -p logs/ ; \
	$(gen_verbose) $(CT_RUN) -suite $(addsuffix _SUITE,$(CT_SUITES_RIAK)) $(CT_OPTS_RIAK)
	rm -rf test/*beam

