const { Transform, Readable } = require("stream");

console.log("[TRADUTOR DE ACESSO RÁPIDO] A iniciar o motor de processamento de fluxo...");
const memoriaInicial = process.memoryUsage().heapUsed;

// Simulador de Leitura de Dados em Escala Contínua (ex: 5000 blocos para teste de stress leve)
class GeradorFluxoDados extends Readable {
    constructor(limiteBlocos, options) {
        super(options);
        this.limiteBlocos = limiteBlocos;
        this.blocoAtual = 0;
    }

    _read(size) {
        if (this.blocoAtual >= this.limiteBlocos) {
            this.push(null); // Fim do fluxo
            return;
        }
        this.blocoAtual++;
        
        const payloadDados = Buffer.from(`BLOCO_DADOS_TRANSMISSAO_${this.blocoAtual}_STREAM_LEVE`);
        this.push(payloadDados);
    }
}

// Motor de Transformação e Processamento Rápido
class MotorTraducaoStream extends Transform {
    _transform(chunk, encoding, callback) {
        try {
            // Fatiamento atômico do bloco de dados sem acumular na RAM
            const meio = Math.floor(chunk.length / 2);
            const ladoEsquerdo = chunk.subarray(0, meio);
            const ladoDireito = chunk.subarray(meio);

            const chunkProcessado = Buffer.concat([Buffer.from(ladoEsquerdo), ladoDireito]);

            this.push(chunkProcessado);
            callback();
        } catch (erro) {
            callback(erro);
        }
    }
}

// Execução do fluxo integrado com simulação de 5000 blocos
const TOTAL_BLOCOS = 5000;
const origemDados = new GeradorFluxoDados(TOTAL_BLOCOS);
const motorTraducao = new MotorTraducaoStream();

let blocosProcessados = 0;

origemDados
    .pipe(motorTraducao)
    .on("data", (chunk) => {
        blocosProcessados++;
        if (blocosProcessados % 1000 === 0) {
            console.log(`[PROCESSAMENTO] ${blocosProcessados} blocos processados com sucesso...`);
        }
    })
    .on("end", () => {
        const memoriaFinal = process.memoryUsage().heapUsed;
        const consumoMistoMB = ((memoriaFinal - memoriaInicial) / 1024 / 1024).toFixed(2);
        
        console.log("\n[SUCESSO] Processamento de fluxo concluído!");
        console.log(`- Total de blocos manipulados: ${TOTAL_BLOCOS}`);
        console.log(`- Variação de memória Heap utilizada: ${consumoMistoMB} MB`);
    });

    