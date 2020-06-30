import * as k8s from "@kubernetes/client-node";
import {K8SCommandJobMessage, K8sExecutorConfig, K8sJobDetails, K8sJobYaml, K8sJobYamlSpec} from "./entities";
import * as O from "fp-ts/lib/Option";
import * as TE from "fp-ts/lib/TaskEither";
import * as E from "fp-ts/lib/Either";
import {defaultContainerName, hyperflowId, k8sJobTemplatePath, k8sNamespace, k8sVolumePath} from "./contants";
import {pipe} from "fp-ts/lib/pipeable";
import {identity, orElse} from "./utils";
import * as yaml from 'js-yaml';
import * as fs from 'fs';
import {parseEnvVariableOpt} from "./job-queue-utils";
import {IncomingMessage} from "http";
import {V1JobSpec} from "@kubernetes/client-node";

const createJobName = (details: K8sJobDetails): string => {
    const randomPath = Math.random().toString(36).substring(7);
    const jobNameExtracted = details.name.replace(/_/g, '-');
    const jobName = `${randomPath}-${jobNameExtracted}-${details.procId}-${details.firingId}`
    return jobName.replace(/[^0-9a-z-]/gi, '').toLowerCase();
}

const extractContainerName = (executor: K8sExecutorConfig): string => {
    return O.getOrElse<string>(() => defaultContainerName)(O.fromNullable(executor.image));
}

const extractCpuRequest = (details: K8sJobDetails): string => {
    return pipe(
        O.fromNullable(details.cpuRequest),
        orElse(() => parseEnvVariableOpt<string>(process.env.HF_VAR_CPU_REQUEST)(identity)),
        O.getOrElse(() => "0.5"),
    )
}

const extractMemRequest = (details: K8sJobDetails): string => {
    return pipe(
        O.fromNullable(details.memRequest),
        orElse(() => parseEnvVariableOpt<string>(process.env.HF_VAR_MEM_REQUEST)(identity)),
        O.getOrElse(() => "50Mi"),
    )
}

const interpolate = (yamlTemplate: string, params: Object): string => {
    return yamlTemplate.replace(/\${(\w+)}/g, (_, v) => params[v]);
}

const createK8sJobSpec = (
    kubeConfig: k8s.KubeConfig,
    redisUrl: string,
    taskIds: string[],
    k8sExecutorConfig: K8sExecutorConfig,
    k8sJobMessage: K8SCommandJobMessage,
): K8sJobYamlSpec => {
    const {details, ins, outs, stdout, stderr, stdoutAppend, stderrAppend, taskId} = k8sJobMessage;

    const command = `hflow-job-execute ${redisUrl} -a ${taskIds.join(' ')}`;
    const containerName = extractContainerName(k8sExecutorConfig);
    const jobName = createJobName(details);

    const cpuRequest = extractCpuRequest(details);
    const memRequest = extractMemRequest(details);

    const jobYamlTemplate = fs.readFileSync(k8sJobTemplatePath, 'utf8');

    const params: Object = {
        command: command,
        containerName: containerName,
        jobName: jobName,
        volumePath: k8sVolumePath,
        cpuRequest: cpuRequest,
        memRequest: memRequest,
        experimentId: hyperflowId,
        workflowName: "wfname",
        taskName: details.name,
    };


    const jobYaml = yaml.safeLoad(interpolate(jobYamlTemplate, params));

    const jobMessage = {
        "executable": k8sExecutorConfig.executable,
        "args": [].concat(),
        "inputs": ins.map(identity),
        "outputs": outs.map(identity),
        "stdout": stdout,
        "stderr": stderr,
        "stdoutAppend": stdoutAppend,
        "stderrAppend": stderrAppend,
        "taskId": taskId,
        "name": details.name,
    }

    return {
        jobYaml: jobYaml,
        jobMessage: jobMessage,
    }
}

const createK8sJob = (k8sApi: k8s.BatchV1Api, jobYaml: K8sJobYaml, taskIds: string[], attempt: number): void => {
    pipe(
        TE.tryCatch(
            () => {
                // @ts-ignore
                return k8sApi.createNamespacedJob(k8sNamespace, jobYaml)
            },
            err => {
                E.tryCatch<number, any>(
                    () => {
                        // @ts-ignore
                        const response: IncomingMessage = err.response;
                        const statusCode: number = response.statusCode
                        switch (statusCode) {
                            // if we get 409 or 429 ==> wait and retry
                            case 409: // 'Conflict' -- "Operation cannot be fulfilled on reourcequotas"; bug in k8s?
                            case 429: // 'Too many requests' -- API overloaded
                                // Calculate delay: default 1s, for '429' we should get it in the 'retry-after' header
                                const delay = pipe(
                                    O.fromNullable(response.headers['retry-after']),
                                    O.chain(value => O.tryCatch(() => Number(value))),
                                    O.getOrElse(() => 1),
                                    value => value * 1000,
                                )
                                console.log("Create k8s job", taskIds, "HTTP error " + statusCode + " (attempt " + attempt +
                                    "), retrying after " + delay + "ms.");

                                setTimeout(() => createK8sJob(k8sApi, jobYaml, taskIds, attempt + 1), delay);
                                break;
                            default:
                                console.error("Err");
                                console.error(err);
                                // console.error(job);
                                let taskEnd = new Date().toISOString();
                                console.log("Task ended with error, time=", taskEnd);
                        }
                    },
                    inErr => {
                        console.error(inErr);
                        throw(inErr);
                    }
                )
            }
        ),
    )
}

export const submitK8sJob = (kubeConfig: k8s.KubeConfig, redisUrl: string, taskIds: string[], k8sExecutorConfig: K8sExecutorConfig, k8sJobMessage: K8SCommandJobMessage): number => {
    const {jobYaml, jobMessage} = createK8sJobSpec(kubeConfig, redisUrl, taskIds, k8sExecutorConfig, k8sJobMessage);

    if (process.env.HF_VAR_K8S_TEST) {
        console.log(JSON.stringify(jobYaml, null, 4));
        console.log(JSON.stringify(jobMessage, null, 2));
        return 0;
    }

    const taskStartDate = new Date().toISOString();
    console.log('Starting tasks', taskIds, 'time=' + taskStartDate);

    const k8sApi = kubeConfig.makeApiClient(k8s.BatchV1Api);

    createK8sJob(k8sApi, jobYaml, taskIds, 1);

}