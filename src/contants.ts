import {createQueueKey, parseEnvVariable, parseEnvVariableOpt} from "./job-queue-utils";
import {identity} from "./utils";
import {HyperflowId} from "./entities";
import * as O from "fp-ts/lib/Option";

export const logLevel = O.getOrElse(() => 'warn')(parseEnvVariableOpt<string>(process.env.HF_LOG_LEVEL)(identity))

export const redisURL = parseEnvVariableOpt<string>(process.env.REDIS_URL)(identity);
export const hyperflowId = parseEnvVariable<HyperflowId>(process.env.HFID)(str => ({hfId: str}));
export const algomeratedTasks = parseEnvVariable<Set<string>>(process.env.HJAQ_AGLOMERATED_TASKS)(str => {
    return new Set(str.split(':'));
});
export const pollingInterval = O.getOrElse(() => 1000)(parseEnvVariableOpt<number>(process.env.HJAQ_POLLING_INTERVAL)(parseInt));
export const maxBatchJobs = O.getOrElse(() => 25)(parseEnvVariableOpt<number>(process.env.HJAQ_MAX_BATCH)(parseInt));
export const jobQueueName = createQueueKey(hyperflowId, 'jq');
export const processingQueueName = createQueueKey(hyperflowId, 'pq');
export const completedQueueName = createQueueKey(hyperflowId, 'cq');

export const k8sVolumePath = '/work_dir';
export const k8sNamespace = O.getOrElse(() => 'default')(parseEnvVariableOpt<string>(process.env.HF_VAR_NAMESPACE)(identity));
export const k8sJobTemplatePath = O.getOrElse(() => './job-template.yaml')(
    parseEnvVariableOpt<string>(process.env.HF_VAR_JOB_TEMPLATE_PATH)(identity)
);