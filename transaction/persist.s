// clwb will be ordered by sfence
TEXT ·sfence(SB),$0
	SFENCE
	RET

// flush cache line
TEXT ·clflush(SB), $0
	MOVQ 	ptr+0(FP), BX
	//CLFLUSH	(BX)
	// loop 10 times instead of call clflush
	MOVQ	$10, DX
loop:
	DECQ	DX
	TESTQ	DX, DX
	JNE		loop
	RET

// persist range of memory
TEXT ·Persist(SB), $0
	MOVQ	start+0(FP), AX
	MOVQ	AX, CX
	ANDQ	$-64, AX
	MOVQ	size+8(FP), DX
	LEAQ	-1(DX)(CX*1), CX
	ANDQ	$-64, CX
flush:
	//CLFLUSH (AX)
	// loop 10 times instead of call clflush
	MOVQ	$10, DX
loop:
	DECQ	DX
	TESTQ	DX, DX
	JNE		loop
	ADDQ	$64, AX
	CMPQ	AX, CX
	JLS		flush
	RET
