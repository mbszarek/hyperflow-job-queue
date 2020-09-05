export interface HyperflowId {
    hfId: string;
}

export interface RedisNotifyKeyspaceEvents {
    'notify-keyspace-events': string,
}

export interface K8sExecutorConfig {
    image: string,
    executable: string,
    args?: string[],
    stdout?: string,
    stderr?: string,
    stdoutAppend?: string,
    stderrAppend?: string,
}

export type K8sJobYaml = string | object;

export interface K8sJobDescription {
    executorConfig: K8sExecutorConfig,
    inputs: string[],
    outputs: string[],
    redis_url: string,
    taskId: string,
    name: string,
    wfname: string,
    procId: string,
    firingId: string,
    runAttempt: number,
}

export interface K8sJobMessage {
    executable: string,
    args: string[],
    inputs: string[],
    outputs: string[],
    stdout?: string,
    stderr?: string,
    stdoutAppend?: string,
    stderrAppend?: string,
    redis_url: string,
    taskId: string,
    name: string,
}

export interface K8sJobYamlSpec {
    jobYaml: K8sJobYaml,
    jobMessage: K8sJobMessage,
}

export interface JobResult {
    message: string,
    code: number,
}