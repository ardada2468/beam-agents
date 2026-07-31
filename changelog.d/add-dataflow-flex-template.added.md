The fraud-triage example now ships as a Dataflow Flex Template
(`examples/fraud_triage_dataflow/`): one `gcloud dataflow flex-template run`
puts it on Dataflow with topics, provider reference and human-approval deadline
supplied as parameters, all in the same URI and `module:object` grammars the
Python and YAML surfaces use. Provider API keys are supplied as Secret Manager
version resource names and resolved on the worker — never as launch parameters.
