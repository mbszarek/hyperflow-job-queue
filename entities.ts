export interface HyperflowId {
    hfId: string;
}

export interface RedisNotifyKeyspaceEvents {
    'notify-keyspace-events': string,
}

export interface K8sExecutorConfig {
    name: string,
    image?: string,
    executable: string,
    args: string[],
    stdout: string,
}

export interface K8sJobDetails {
    name: string,
    procId: string,
    firingId: string,
    cpuRequest?: string,
    memRequest?: string,
}

export interface K8SCommandJobMessage {
    executorConfig: K8sExecutorConfig
    details: K8sJobDetails,
    ins: string[],
    outs: string[],
    stdout?: string,
    stderr?: string,
    stdoutAppend?: string,
    stderrAppend?: string,
    taskId: string,
}

export type K8sJobYaml = string | object;

export interface K8sJobYamlSpec {
    jobYaml: K8sJobYaml,
    jobMessage: object,
}