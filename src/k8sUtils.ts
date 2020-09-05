import * as k8s from "@kubernetes/client-node";
import {K8sExecutorConfig, K8sJobDescription, K8sJobMessage, K8sJobYaml, K8sJobYamlSpec} from "./entities";
import * as O from "fp-ts/lib/Option";
import * as TE from "fp-ts/lib/TaskEither";
import * as E from "fp-ts/lib/Either";
import {hyperflowId, k8sJobTemplatePath, k8sNamespace, k8sVolumePath} from "./contants";
import {pipe} from "fp-ts/lib/pipeable";
import {getJobResult, identity, orElse} from "./utils";
import * as yaml from 'js-yaml';
import * as fs from 'fs';
import {createJobDescriptionKey, parseEnvVariableOpt} from "./job-queue-utils";
import {IncomingMessage} from "http";
import * as redis from "redis";
import {from, Observable, of} from "rxjs";
import {flatMap, groupBy, map, mergeMap, toArray} from "rxjs/operators";

type DockerImage = string;

const createJobName = (jobMessage: K8sJobDescription): string => {
    const randomPath = Math.random().toString(36).substring(7);
    const jobNameExtracted = jobMessage.name.replace(/_/g, '-');
    const jobName = `${randomPath}-${jobNameExtracted}-${jobMessage.procId}-${jobMessage.firingId}`
    return jobName.replace(/[^0-9a-z-]/gi, '').toLowerCase();
}

const extractCpuRequest = (cpuRequest?: string): string => {
    return pipe(
        O.fromNullable(cpuRequest),
        orElse(() => parseEnvVariableOpt<string>(process.env.HF_VAR_CPU_REQUEST)(identity)),
        O.getOrElse(() => "0.5"),
    )
}

const extractMemRequest = (memRequest?: string): string => {
    return pipe(
        O.fromNullable(memRequest),
        orElse(() => parseEnvVariableOpt<string>(process.env.HF_VAR_MEM_REQUEST)(identity)),
        O.getOrElse(() => "50Mi"),
    )
}

const interpolate = (yamlTemplate: string, params: Object): string => {
    return yamlTemplate.replace(/\${(\w+)}/g, (_, v) => params[v]);
}

const sendMsgToJob = async (message: K8sJobMessage, taskId: string, redisClient: redis.RedisClient): Promise<number> => {
    return new Promise<number>((resolve, reject) => {
        const taskMessageKey = `${taskId}_msg`
        redisClient.lpush(taskMessageKey, JSON.stringify(message), (err, reply) => {
            err ? reject(err) : resolve(reply)
        })
    })
}

const getJobDescription = async (taskId: string, redisClient: redis.RedisClient): Promise<K8sJobDescription> => {
    const jobDescriptionJson = await new Promise<string>((resolve, reject) => {
        redisClient.lrange(createJobDescriptionKey(taskId), 0, -1, (err, reply) => {
            err ? reject(err) : resolve(reply[0]);
        })
    })
    return JSON.parse(jobDescriptionJson) as K8sJobDescription;
}

const createK8sJobMessage = (jobDescription: K8sJobDescription): K8sJobMessage => {
    const {
        executorConfig,
        inputs,
        outputs,
        redis_url,
        taskId,
        name,
    } = jobDescription;

    const {
        executable,
        args,
        stdout,
        stderr,
        stdoutAppend,
        stderrAppend,
    } = executorConfig;

    return {
        "executable": executable,
        "args": [].concat(args),
        "inputs": inputs,
        "outputs": outputs,
        "stdout": stdout,
        "stderr": stderr,
        "stdoutAppend": stdoutAppend,
        "stderrAppend": stderrAppend,
        "redis_url": redis_url,
        "taskId": taskId,
        "name": name,
    }
}


const createK8sJobSpec = async (
    kubeConfig: k8s.KubeConfig,
    redisClient: redis.RedisClient,
    redisUrl: string,
    taskIds: string[],
    k8sExecutorConfig: K8sExecutorConfig,
    jobDescription: K8sJobDescription,
    cpuRequest?: string,
    memRequest?: string
): Promise<K8sJobYamlSpec> => {

    const {
        executorConfig,
        name,
        wfname,
    } = jobDescription;

    const {image} = executorConfig;

    const command = `hflow-job-execute ${redisUrl} -a ${taskIds.join(' ')}`;
    const jobName = createJobName(jobDescription);

    const extractedCpuRequest = extractCpuRequest(cpuRequest);
    const extractedMemRequest = extractMemRequest(memRequest);

    const jobYamlTemplate = fs.readFileSync(k8sJobTemplatePath, 'utf8');

    const params: Object = {
        command: command,
        containerName: image,
        jobName: jobName,
        volumePath: k8sVolumePath,
        cpuRequest: extractedCpuRequest,
        memRequest: extractedMemRequest,
        experimentId: hyperflowId,
        workflowName: wfname,
        taskName: name,
    };


    const jobYaml = yaml.safeLoad(interpolate(jobYamlTemplate, params));

    const jobMessage = createK8sJobMessage(jobDescription);

    return {
        jobYaml: jobYaml,
        jobMessage: jobMessage,
    }
}

const segregateJobs = async (taskIds: string[], redisClient: redis.RedisClient): Promise<[DockerImage, K8sJobDescription[]][]> => {
    return from(taskIds).pipe(
        flatMap(taskId => from(getJobDescription(taskId, redisClient))),
        groupBy(jobDescription => jobDescription.executorConfig.image),
        flatMap(group => {
            return group.pipe(
                toArray(),
                flatMap(value => of<[DockerImage, K8sJobDescription[]]>([group.key, value]))
            );
        }),
        toArray()
    ).toPromise();
}

const createK8sJob = async (k8sApi: k8s.BatchV1Api, jobYaml: K8sJobYaml, taskIds: string[], attempt: number): Promise<void> => {
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
};

const awaitK8sJob = async (taskIds: string[], redisClient: redis.RedisClient): Promise<number> => {
    const jobResult = await getJobResult(redisClient, taskIds[0], 0);
    return 2;
};

export const submitK8sJob = async (kubeConfig: k8s.KubeConfig, redisClient: redis.RedisClient, redisUrl: string, taskIds: string[], k8sExecutorConfig: K8sExecutorConfig, k8sJobMessage: K8sJobDescription): Promise<number> => {
    from(segregateJobs(taskIds, redisClient)).pipe(
        flatMap(value => value),
        flatMap(value => {
            const [_, jobDescription] = value;
            const xd = createK8sJobSpec(
                kubeConfig,
                redisClient,
                redisUrl,
                jobDescription.map(value => value.taskId ),

            )
            return null;
        })
    )
    const segregatedJobs = await segregateJobs(taskIds, redisClient);

    const {jobYaml, jobMessage} = await createK8sJobSpec(kubeConfig, redisClient, redisUrl, taskIds, k8sExecutorConfig, k8sJobMessage);

    if (process.env.HF_VAR_K8S_TEST) {
        console.log(JSON.stringify(jobYaml, null, 4));
        console.log(JSON.stringify(jobMessage, null, 2));
        return 0;
    }

    const taskStartDate = new Date().toISOString();
    console.log('Starting tasks', taskIds, 'time=' + taskStartDate);

    const k8sApi = kubeConfig.makeApiClient(k8s.BatchV1Api);

    await createK8sJob(k8sApi, jobYaml, taskIds, 1);

    const redisResult = await sendMsgToJob(jobMessage, taskIds[0], null);

}