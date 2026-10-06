// Ambient stubs for the example-only dependencies (see tsconfig.json here).
//
// These are intentionally loose. The point of type-checking the examples in CI
// is to catch drift in the CollieAi SDK's own surface — a renamed field, a
// changed event shape. Pulling `express` and `openai` (plus @types) into the
// SDK's devDependencies to type-check two demo files would cost every CI run a
// heavy install and buy nothing: nobody ships a regression to the SDK by
// changing Express.
//
// If an example ever needs real provider types, install the package and delete
// the matching declaration here rather than tightening these by hand.

declare module "express" {
  // Handlers get `any` req/res on purpose — see the note above. Typed as a
  // callable with an explicit signature so `strict`'s noImplicitAny doesn't
  // flag the example's own handler parameters.
  interface Application {
    use(...handlers: unknown[]): void;
    post(path: string, handler: (req: any, res: any) => unknown): void;
    listen(port: number, cb?: () => void): void;
  }
  interface Express {
    (): Application;
    json(): unknown;
  }
  const express: Express;
  export default express;
}

declare module "openai" {
  export default class OpenAI {
    constructor(opts?: any);
    chat: any;
  }
}
