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

interface K8sJobYamlSpec {
    jobYaml: K8sJobYaml
}

export interface K8sMultiJobYamlSpec extends K8sJobYamlSpec {
    jobMessages: Array<K8sJobMessage>,
}

export interface K8sSingleJobYamlSpec extends K8sJobYamlSpec {
    jobMessage: K8sJobMessage,
}

export interface JobResult {
    message: string,
    code: number,
}