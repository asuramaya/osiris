
declare module '@deepseek-ai/cordis' {
  export interface Context {
    get(name: string): any;
    logger: any;
    on(event: string, handler: (...args: any[]) => any): void;
  }
}

declare module '@deepseek-ai/schemastery' {
  export const Schema: {
    object(def: Record<string, any>): any;
    string(): any;
    number(): any;
    boolean(): any;
  };
  export type Schema<T> = any;
}

declare const process: {
  cwd(): string;
};
