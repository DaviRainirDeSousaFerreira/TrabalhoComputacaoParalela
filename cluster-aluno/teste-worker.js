// Importa a classe Worker.
import { Worker } from "node:worker_threads";

// Cria um worker utilizando o arquivo worker.js.
const worker = new Worker(new URL("./worker.js", import.meta.url), {
  workerData: {
    inicio: 1,
    fim: 10,
  },
});

// Recebe o resultado enviado pelo worker.
worker.on("message", (dados) => {
  console.log("Resultado recebido:", dados.resultado);
  console.log("Tempo de execução:", dados.kernelMs, "ms");
});

// Identifica possíveis erros.
worker.on("error", (erro) => {
  console.error("Erro no worker:", erro);
});

// Informa quando o worker termina.
worker.on("exit", (codigo) => {
  console.log("Worker finalizado. Código:", codigo);
});
