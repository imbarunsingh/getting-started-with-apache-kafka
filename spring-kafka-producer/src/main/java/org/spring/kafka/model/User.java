package org.spring.kafka.model;

import java.io.Serializable;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class User implements Serializable {

	private static final long serialVersionUID = -5960773371510686796L;

	private String name;
	private String dept;
	private Long salary;
}
